package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// GeckoCoin API에서 사용하는 코인 마켓 캡 정보를 나타내는 구조체
type GeckoCoin struct {
	Symbol    string `json:"symbol"`
	MarketCap int64  `json:"market_cap"`
}

// UpdateMarketIndex 함수는 상위 10개 코인의 마켓 캡 정보를 가져와서 마켓 인덱스와 알트 인덱스를 계산하고, 이를 데이터베이스에 삽입합니다.
func UpdateMarketIndex(ctx context.Context, db *sql.DB) error {
	// 상위 10개 코인의 마켓 캡 정보를 가져옵니다.
	coinCaps, err := getMarketCap(ctx)
	if err != nil {
		return fmt.Errorf("getMarketCap 에러: %v", err)
	}
	// 마켓 인덱스와 알트 인덱스를 계산합니다.
	marketIndex, altIndex := calcMarketIndex(coinCaps)

	// 데이터베이스에 마켓 인덱스와 알트 인덱스를 삽입합니다.
	if err := insertMarketIndex(ctx, db, marketIndex, altIndex); err != nil {
		return fmt.Errorf("insertMarketIndex 에러: %v", err)
	}

	return nil
}

// getMarketCap 함수는 CoinGecko API를 사용하여 상위 10개 코인의 마켓 캡 정보를 가져옵니다.
func getMarketCap(ctx context.Context) ([]GeckoCoin, error) {
	apiURL := "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50"

	// 제외할 종목 심볼 목록 (string 배열)
	excludedSymbols := []string{
		"usdt",
		"bnb",
		"usdc",
		"steth",
		"wbtc",
		"wsteth",
		"leo",
		"usds",
		"weth",
		"weeth",
		"bsc-usd",
		"bgb",
		"usde",
		"cbbtc",
		"wbt",
		"okb",
		"jitosol",
		"susde",
		"tkx",
		"buidl",
		"ondo",
		"cro",
	}

	// Context를 활용한 HTTP 요청 (타임아웃: 30초)
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("HTTP 요청 생성 에러: %v", err)
	}

	// API 요청 보내기
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API 요청 에러: %v", err)
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	// 응답 본문 읽기
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("응답 본문 읽기 에러: %v", err)
	}

	// JSON 응답을 Coin 슬라이스로 디코딩
	var coins []GeckoCoin
	err = json.Unmarshal(body, &coins)
	if err != nil {
		return nil, fmt.Errorf("JSON 디코딩 에러: %v", err)
	}

	// 제외할 종목 필터링
	var filteredCoins []GeckoCoin
	for _, coin := range coins {
		isExcluded := false
		for _, excludedSymbol := range excludedSymbols {
			if coin.Symbol == excludedSymbol {
				isExcluded = true
				break
			}
		}
		if !isExcluded {
			filteredCoins = append(filteredCoins, coin)
		}
	}

	// 상위 10개 종목 저장
	top10Coins := make([]GeckoCoin, 0, 10)
	for i, coin := range filteredCoins {
		if i >= 10 {
			break
		}
		top10Coins = append(top10Coins, coin)
	}

	// 결과 반환
	return top10Coins, nil
}

// calcMarketIndex 함수는 상위 10개 코인의 마켓 캡 정보를 기반으로 마켓 인덱스와 알트 인덱스를 계산합니다.
func calcMarketIndex(coinCaps []GeckoCoin) (int, int) {
	marketIndex := 0
	altIndex := 0
	for i, coinCap := range coinCaps {
		if i > 0 {
			altIndex += int(coinCap.MarketCap)
		}
		marketIndex += int(coinCap.MarketCap)
	}
	marketIndex /= 100000000
	altIndex /= 100000000
	return marketIndex, altIndex
}

// insertMarketIndex 함수는 계산된 마켓 인덱스와 알트 인덱스를 데이터베이스에 삽입합니다.
func insertMarketIndex(ctx context.Context, db *sql.DB, marketIndex, altIndex int) error {
	// 현재 날짜와 시간을 가져옵니다.
	now := time.Now().Round(time.Hour)
	date := now.Format("2006-01-02")
	hour := now.Hour()

	// 트랜잭션 시작
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("트랜잭션 시작 에러: %v", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// 데이터베이스에 마켓 인덱스와 알트 인덱스를 업데이트하는 쿼리
	query := `
		INSERT INTO market_indices (date, hour, market_index, alt_index)
		VALUES (?, ?, ?, ?)
	`
	// Context를 활용한 쿼리 실행 (타임아웃: 10초)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err = tx.ExecContext(queryCtx, query, date, hour, marketIndex, altIndex)
	if err != nil {
		return fmt.Errorf("데이터베이스 업데이트 에러: %v", err)
	}

	// 트랜잭션 커밋
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("트랜잭션 커밋 에러: %v", err)
	}

	return nil
}
