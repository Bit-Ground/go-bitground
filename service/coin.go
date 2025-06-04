package service

import (
	"Bitground-go/model"
	"Bitground-go/util"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"net/http"
	"time"
)

func UpdateCoins(ctx context.Context, db *sql.DB) error {
	// 고루틴 사용
	var coinList []model.UpbitCoinList
	var coinDetailList map[string]model.UpbitCoinPrice

	g, gCtx := errgroup.WithContext(ctx)
	// 1. API에서 코인 정보를 가져와 "KRW-"로 시작하는 코인 심볼을 필터링 (비동기)
	g.Go(func() error {
		symbols, err := getSymbolList(gCtx) // gCtx를 사용
		if err != nil {
			return fmt.Errorf("getSymbolList 에러: %w", err)
		}
		coinList = symbols // 결과 저장
		return nil
	})

	// 2. 심볼에 해당하는 코인 정보들 가져오기
	// 3. DB에서 코인 심볼 목록을 조회하여 is_deleted 업데이트
	g.Go(func() error {
		details, err := getCoinDetails(gCtx) // gCtx를 사용
		if err != nil {
			return fmt.Errorf("getCoinDetails 에러: %w", err)
		}
		// getCoinDetails가 성공적으로 완료된 후에 updateDeletedVal을 호출합니다.
		if err := updateDeletedVal(gCtx, db, details); err != nil { // gCtx를 사용
			return fmt.Errorf("updateDeletedVal 에러: %w", err)
		}
		coinDetailList = details // 결과 저장
		return nil
	})

	if err := g.Wait(); err != nil {
		// 에러가 발생하면 여기서 처리하고 함수를 종료합니다.
		return fmt.Errorf("고루틴 수행 중 에러 발생: %w", err)
	}

	// 4. 데이터들을 가공하여 db에 저장할 수 있는 형태로 변환
	coinSymbols := prepareCoinSymbols(coinList, coinDetailList)

	// 5. db에 코인 심볼 목록 저장
	if err := insertCoinSymbols(ctx, db, coinSymbols); err != nil {
		return fmt.Errorf("insertCoinSymbols 에러: %w", err)
	}

	return nil
}

// api에서 코인 전체 정보를 가져와 "KRW-" 로 시작하는 코인 심볼을 필터링합니다.
func getSymbolList(ctx context.Context) ([]model.UpbitCoinList, error) {
	apiURL := "https://api.upbit.com/v1/market/all?is_details=true"

	// Context를 활용한 HTTP 요청 (타임아웃: 10초)
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("HTTP 요청 생성 에러: %w", err)
	}

	// API 요청 보내기
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API 요청 에러: %w", err)
	}
	defer func(Body io.ReadCloser) {
		// 응답 본문을 닫아 리소스 누수 방지
		if err := Body.Close(); err != nil {
			log.Printf("응답 본문 닫기 에러: %v\n", err)
		}
	}(resp.Body)

	// JSON 응답을 Coin 슬라이스로 디코딩
	var coins []model.UpbitCoinList
	err = json.NewDecoder(resp.Body).Decode(&coins)
	if err != nil {
		return nil, fmt.Errorf("JSON 디코딩 에러: %w", err)
	}

	// "KRW-"로 시작하는 코인 심볼 필터링
	var filteredCoins []model.UpbitCoinList
	for _, coin := range coins {
		if len(coin.Market) >= 4 && coin.Market[:4] == "KRW-" {
			filteredCoins = append(filteredCoins, coin)
		}
	}

	return filteredCoins, nil

}

// api에서 코인별 정보를 가져와 정보를 구체화합니다.
func getCoinDetails(ctx context.Context) (map[string]model.UpbitCoinPrice, error) {
	apiURL := "https://api.upbit.com/v1/ticker/all?quote_currencies=KRW"

	// Context를 활용한 HTTP 요청 (타임아웃: 10초)
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("HTTP 요청 생성 에러: %w", err)
	}

	// API 요청 보내기
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API 요청 에러: %w", err)
	}
	defer func(Body io.ReadCloser) {
		if err := Body.Close(); err != nil {
			log.Printf("응답 본문 닫기 에러: %v\n", err)
		}
	}(resp.Body)

	// 응답 본문 읽기
	var coinDetails []model.UpbitCoinPrice
	err = json.NewDecoder(resp.Body).Decode(&coinDetails)
	if err != nil {
		return nil, fmt.Errorf("JSON 디코딩 에러: %w", err)
	}

	// 코인 심볼을 키로 하는 맵 생성
	coinMap := make(map[string]model.UpbitCoinPrice)
	for _, coin := range coinDetails {
		coinMap[coin.Market] = coin
	}

	return coinMap, nil
}

// 데이터들을 가공하여 db에 저장할 수 있는 형태로 변환합니다.
func prepareCoinSymbols(coinList []model.UpbitCoinList, coinDetailList map[string]model.UpbitCoinPrice) []model.CoinSymbol {
	var coinSymbols []model.CoinSymbol

	for _, coin := range coinList {
		if detail, exists := coinDetailList[coin.Market]; exists {
			// 코인 심볼 구조체 생성
			symbol := model.CoinSymbol{
				Symbol:     coin.Market,
				KoreanName: coin.KoreanName,
				TradePrice: int64(detail.AccTradePrice),
				IsCaution: coin.MarketEvent.Caution.PriceFluctuations || coin.MarketEvent.Caution.TradingVolumeSoaring ||
					coin.MarketEvent.Caution.DepositAmountSoaring || coin.MarketEvent.Caution.GlobalPriceDifferences ||
					coin.MarketEvent.Caution.ConcentrationOfSmallAccounts,
				IsWarning: coin.MarketEvent.Warning,
			}

			// ChangeRate 계산
			if detail.PrevClosingPrice != 0 {
				symbol.ChangeRate = (detail.TradePrice - detail.PrevClosingPrice) / detail.PrevClosingPrice * 100
			}

			coinSymbols = append(coinSymbols, symbol)
		}
	}

	return coinSymbols
}

// db에서 코인 심볼 목록을 조회합니다.
// coinDetailList에 있는데 is_deleted가 true라면 false로 업데이트
// coinDetailList에 없는데 is_deleted가 false라면 true로 업데이트
func updateDeletedVal(ctx context.Context, db *sql.DB, coinDetailList map[string]model.UpbitCoinPrice) error {
	// 쿼리 타임아웃 설정 (20초)
	queryCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	// 트랜잭션 시작
	tx, err := db.BeginTx(queryCtx, nil)
	if err != nil {
		return fmt.Errorf("트랜잭션 시작 에러: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		} else if err != nil {
			_ = tx.Rollback()
		}
	}()

	query := `
		SELECT symbol, is_deleted
		FROM coins
    `

	// 쿼리 실행
	rows, err := tx.QueryContext(queryCtx, query)
	if err != nil {
		return fmt.Errorf("쿼리 실행 에러: %w", err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			log.Printf("Rows 닫기 에러: %v\n", err)
		}
	}(rows)

	var symbolsToSetTrue []string
	var symbolsToSetFalse []string

	for rows.Next() {
		var testSymbol model.CoinSymbol
		if err := rows.Scan(&testSymbol.Symbol, &testSymbol.IsDeleted); err != nil {
			return fmt.Errorf("rows.Scan 에러: %w", err)
		}

		// coinDetailList에 있는지 확인
		if _, exists := coinDetailList[testSymbol.Symbol]; exists {
			// coinDetailList에 있는데 is_deleted가 true라면 false로 업데이트
			if testSymbol.IsDeleted {
				symbolsToSetFalse = append(symbolsToSetFalse, testSymbol.Symbol)
			}
		} else {
			// coinDetailList에 없는데 is_deleted가 false라면 true로 업데이트
			if !testSymbol.IsDeleted {
				symbolsToSetTrue = append(symbolsToSetTrue, testSymbol.Symbol)
			}
		}
	}

	if err := rows.Err(); err != nil { // rows.Err()로 반복 중 발생한 에러 확인
		return fmt.Errorf("rows 반복 중 에러: %w", err)
	}

	// is_deleted를 true로 업데이트할 심볼들이 있다면 배치 업데이트
	if len(symbolsToSetTrue) > 0 {
		args := make([]interface{}, len(symbolsToSetTrue))
		for i, symbol := range symbolsToSetTrue {
			args[i] = symbol
		}

		updateQuery := fmt.Sprintf(`UPDATE coins SET is_deleted = true WHERE symbol IN (%s)`,
			util.GeneratePlaceholders(len(symbolsToSetTrue)))

		if _, err := tx.ExecContext(queryCtx, updateQuery, args...); err != nil {
			return fmt.Errorf("is_deleted = true 업데이트 에러: %w", err)
		}
	}

	// is_deleted를 false로 업데이트할 심볼들이 있다면 배치 업데이트
	if len(symbolsToSetFalse) > 0 {
		args := make([]interface{}, len(symbolsToSetFalse))
		for i, symbol := range symbolsToSetFalse {
			args[i] = symbol
		}

		updateQuery := fmt.Sprintf(`UPDATE coins SET is_deleted = false WHERE symbol IN (%s)`,
			util.GeneratePlaceholders(len(symbolsToSetFalse)))

		if _, err := tx.ExecContext(queryCtx, updateQuery, args...); err != nil {
			return fmt.Errorf("is_deleted = false 업데이트 에러: %w", err)
		}
	}

	return tx.Commit() // 트랜잭션 커밋
}

// db에 코인 심볼 목록을 저장합니다.
func insertCoinSymbols(ctx context.Context, db *sql.DB, coinSymbols []model.CoinSymbol) error {

	// 쿼리 타임아웃 설정 (10초)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 트랜잭션 시작
	tx, err := db.BeginTx(queryCtx, nil)
	if err != nil {
		return fmt.Errorf("트랜잭션 시작 에러: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		} else if err != nil {
			_ = tx.Rollback()
		}
	}()

	query := `
		INSERT INTO coins (symbol, korean_name, trade_price_24h, change_rate, is_caution, is_warning)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			korean_name = VALUES(korean_name),
			trade_price_24h = VALUES(trade_price_24h),
			change_rate = VALUES(change_rate),
			is_caution = VALUES(is_caution),
			is_warning = VALUES(is_warning)
	`
	stmt, err := tx.PrepareContext(queryCtx, query)
	if err != nil {
		return fmt.Errorf("쿼리 준비 에러: %w", err)
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			log.Printf("쿼리 종료 에러: %v\n", err)
		}
	}()

	for _, symbol := range coinSymbols {
		if _, err := stmt.ExecContext(queryCtx,
			symbol.Symbol, symbol.KoreanName, symbol.TradePrice,
			symbol.ChangeRate, symbol.IsCaution, symbol.IsWarning); err != nil {
			return fmt.Errorf("코인 심볼 삽입/업데이트 에러: %w", err)
		}
	}

	return tx.Commit() // 트랜잭션 커밋
}
