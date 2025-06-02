package service

import (
	"Bitground-go/model"
	"Bitground-go/util"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

func UpdateRank(ctx context.Context, db *sql.DB, symbolMap map[string]int, seasonID int) error {
	// 1. 시즌 참여중인 유저 ID 목록 가져오기
	userIDs, err := getParticipatingUserIDs(ctx, db, seasonID)
	if err != nil {
		return fmt.Errorf("유저 ID 조회 실패: %w", err)
	}

	if len(userIDs) == 0 {
		return nil // 참여중인 유저가 없으면 종료
	}

	// 2. 모든 코인의 현재가 가져오기
	coinPrices, err := getAllCoinsCurrentPrice(ctx, symbolMap)
	if err != nil {
		return fmt.Errorf("모든 코인의 현재가 조회 실패: %w", err)
	}

	if len(coinPrices) == 0 {
		return fmt.Errorf("활성화된 코인이 없습니다")
	}

	// 고루틴 사용하여 병렬 처리
	cashMapChan := make(chan map[int]int, 1)
	assetsMapChan := make(chan map[int][]model.UserAsset, 1)
	errChan := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2) // 2개의 고루틴을 사용하여 현금과 자산 정보를 병렬로 조회

	// 3. 모든 유저의 현금 정보 조회 (비동기)
	go func() {
		defer wg.Done()
		cash, getCashErr := getUserCashMap(ctx, db, userIDs)
		if getCashErr != nil {
			errChan <- fmt.Errorf("유저 현금 정보 조회 실패: %w", getCashErr)
			return
		}
		cashMapChan <- cash
	}()

	// 4. 모든 유저의 자산 정보 조회 (비동기)
	go func() {
		defer wg.Done()
		assets, getAssetsErr := getUserAssetsMap(ctx, db, userIDs)
		if getAssetsErr != nil {
			errChan <- fmt.Errorf("유저 자산 정보 조회 실패: %w", getAssetsErr)
			return
		}
		assetsMapChan <- assets
	}()

	// 고루틴이 완료될 때까지 대기
	wg.Wait()
	close(cashMapChan)
	close(assetsMapChan)
	close(errChan)

	// 에러 처리
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	// 채널에서 결과 가져오기
	userCashMap := <-cashMapChan
	userAssetsMap := <-assetsMapChan

	// 5. 각 유저의 총 자산 계산
	var userTotalAssets []model.UserTotalAsset
	for _, userID := range userIDs {
		totalValue := calculateUserTotalValue(userID, userCashMap, userAssetsMap, coinPrices)
		userTotalAssets = append(userTotalAssets, model.UserTotalAsset{
			UserID:     userID,
			TotalValue: totalValue,
		})
	}

	// 6. 랭킹 계산 (총 자산 기준 내림차순 정렬)
	sort.Slice(userTotalAssets, func(i, j int) bool {
		return userTotalAssets[i].TotalValue > userTotalAssets[j].TotalValue
	})

	// 7. 랭크와 티어 할당
	for i := range userTotalAssets {
		userTotalAssets[i].Rank = i + 1
		userTotalAssets[i].Tier = calculateTier(i+1, len(userTotalAssets))
	}

	// 8. 데이터베이스 업데이트
	err = updateUserRankings(ctx, db, userTotalAssets, seasonID)
	if err != nil {
		return fmt.Errorf("유저 랭킹 업데이트 실패: %w", err)
	}

	return nil

}

// GetCurrentSeasonID 진행중인 시즌 ID 가져오기
func GetCurrentSeasonID(ctx context.Context, db *sql.DB) (int, error) {
	// 쿼리 타임아웃 설정 (10초)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `
		SELECT id
		FROM seasons
		WHERE status = 'PENDING'
		LIMIT 1
	`

	// 쿼리 실행
	var seasonID int
	err := db.QueryRowContext(queryCtx, query).Scan(&seasonID)
	if err != nil {
		return 0, fmt.Errorf("쿼리 실행 에러: %w", err)
	}

	return seasonID, nil
}

// 시즌 참여중 userId 목록 가져오기
func getParticipatingUserIDs(ctx context.Context, db *sql.DB, seasonId int) ([]int, error) {
	// 쿼리 타임아웃 설정 (10초)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `
    	SELECT DISTINCT user_id
    	FROM orders
    	WHERE season_id = ? AND status = 'COMPLETED'
	`

	// 쿼리 실행
	rows, err := db.QueryContext(queryCtx, query, seasonId)
	if err != nil {
		return nil, fmt.Errorf("쿼리 실행 에러: %w", err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			log.Printf("행 닫기 에러: %v\n", err)
		}
	}(rows)

	var userIDs []int
	for rows.Next() {
		var userID int
		if err := rows.Scan(&userID); err != nil {
			return nil, fmt.Errorf("행 스캔 에러: %w", err)
		}
		userIDs = append(userIDs, userID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("행 반복 에러: %w", err)
	}

	return userIDs, nil
}

// GetActiveCoinsSymbols 데이터베이스에서 활성화된 코인 심볼들을 가져오는 함수
func GetActiveCoinsSymbols(ctx context.Context, db *sql.DB) (map[string]int, error) {
	// 쿼리 타임아웃 설정 (10초)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `
       SELECT id, symbol
       FROM coins
       WHERE is_deleted = 0
       ORDER BY symbol;
   `

	rows, err := db.QueryContext(queryCtx, query)
	if err != nil {
		return nil, fmt.Errorf("쿼리 실행 에러: %w", err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			log.Printf("행 닫기 에러: %v\n", err)
		}
	}(rows)

	symbolMap := make(map[string]int)
	for rows.Next() {
		var coinSymbol model.CoinSymbol
		err := rows.Scan(&coinSymbol.Id, &coinSymbol.Symbol)
		if err != nil {
			return nil, fmt.Errorf("행 스캔 에러: %w", err)
		}
		symbolMap[coinSymbol.Symbol] = coinSymbol.Id
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("행 반복 에러: %w", err)
	}

	return symbolMap, nil
}

// upbit API를 사용하여 모든 코인의 현재가를 가져오는 함수
func getAllCoinsCurrentPrice(ctx context.Context, symbolMap map[string]int) (map[int]float64, error) {
	apiURL := "https://api.upbit.com/v1/ticker/all?quote_currencies=KRW"

	// Context를 활용한 HTTP 요청 (타임아웃: 10초)
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("HTTP 요청 생성 에러: %w", err)
	}

	// 1. 업비트 API 호출
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

	// 2. API 응답 디코딩
	var upbitPrices []model.UpbitCoinPrice
	err = json.NewDecoder(resp.Body).Decode(&upbitPrices)
	if err != nil {
		return nil, fmt.Errorf("JSON 디코딩 에러: %w", err)
	}

	// 3. 업비트 데이터와 데이터베이스 심볼 매칭
	coinPrices := make(map[int]float64)
	for _, upbitCoin := range upbitPrices {
		// market에서 심볼 추출
		symbol := upbitCoin.Market

		// 데이터베이스에서 해당 심볼의 ID 찾기
		if symbolId, exists := symbolMap[symbol]; exists {
			coinPrices[symbolId] = upbitCoin.TradePrice
		}
	}

	return coinPrices, nil
}

// 유저별 현금 정보 조회
func getUserCashMap(ctx context.Context, db *sql.DB, userIDs []int) (map[int]int, error) {

	// 타임아웃 설정 (10초)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// IN 절을 위한 placeholder 생성
	placeholders := make([]interface{}, len(userIDs))
	for i, id := range userIDs {
		placeholders[i] = id
	}

	query := fmt.Sprintf("SELECT id, cash FROM users WHERE id IN (%s)", util.GeneratePlaceholders(len(userIDs)))
	rows, err := db.QueryContext(queryCtx, query, placeholders...)
	if err != nil {
		return nil, fmt.Errorf("쿼리 실행 에러: %w", err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			log.Printf("행 닫기 에러: %v\n", err)
		}
	}(rows)

	userCashMap := make(map[int]int)
	for rows.Next() {
		var userCash model.UserCash
		err := rows.Scan(&userCash.UserID, &userCash.Cash)
		if err != nil {
			return nil, fmt.Errorf("행 스캔 에러: %w", err)
		}
		userCashMap[userCash.UserID] = userCash.Cash
	}

	return userCashMap, nil
}

// 유저별 자산 정보 조회
func getUserAssetsMap(ctx context.Context, db *sql.DB, userIDs []int) (map[int][]model.UserAsset, error) {
	// 타임아웃 설정 (10초)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	placeholders := make([]interface{}, len(userIDs))
	for i, id := range userIDs {
		placeholders[i] = id
	}

	query := fmt.Sprintf("SELECT user_id, symbol_id, amount FROM user_assets WHERE user_id IN (%s)", util.GeneratePlaceholders(len(userIDs)))
	rows, err := db.QueryContext(queryCtx, query, placeholders...)
	if err != nil {
		return nil, fmt.Errorf("쿼리 실행 에러: %w", err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			log.Printf("행 닫기 에러: %v\n", err)
		}
	}(rows)

	userAssetsMap := make(map[int][]model.UserAsset)
	for rows.Next() {
		var asset model.UserAsset
		err := rows.Scan(&asset.UserID, &asset.SymbolID, &asset.Amount)
		if err != nil {
			return nil, fmt.Errorf("행 스캔 에러: %w", err)
		}
		userAssetsMap[asset.UserID] = append(userAssetsMap[asset.UserID], asset)
	}

	return userAssetsMap, nil
}

// 개별 유저의 총 자산 계산
func calculateUserTotalValue(userID int, userCashMap map[int]int, userAssetsMap map[int][]model.UserAsset, priceMap map[int]float64) int {
	// 현금
	cash := userCashMap[userID]
	totalValue := float64(cash)

	// 코인 자산
	assets := userAssetsMap[userID]
	for _, asset := range assets {
		if price, exists := priceMap[asset.SymbolID]; exists {
			totalValue += asset.Amount * price
		}
	}

	return int(totalValue)
}

// 티어 계산 (랭킹 기준으로 1~5 티어 부여)
func calculateTier(rank, totalUsers int) int {
	// 7티어: 1등
	if rank == 1 {
		return 7
	}

	// 6티어: 2, 3등
	if rank == 2 || rank == 3 {
		return 6
	}

	// 1~3등을 제외한 나머지 인원 계산
	remainingUsers := totalUsers - 3
	if remainingUsers <= 0 { // 1~3등이 전체 인원과 같거나 더 많을 경우
		return 1 // 모든 인원이 1~3등 티어에 해당하므로 나머지 티어는 1티어로 처리
	}
	relativeRank := rank - 3
	if relativeRank <= 0 {
		return 1
	}

	percentage := float64(relativeRank) / float64(remainingUsers)

	switch {
	case percentage <= 0.10: // 상위 10%
		return 5
	case percentage <= 0.25: // 상위 25%
		return 4
	case percentage <= 0.45: // 상위 45%
		return 3
	case percentage <= 0.70: // 상위 70%
		return 2
	default: // 나머지
		return 1
	}
}

// 유저 랭킹 정보 업데이트
func updateUserRankings(ctx context.Context, db *sql.DB, userTotalAssets []model.UserTotalAsset, seasonID int) error {
	// 트랜잭션 시작
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("트랜잭션 시작 에러: %w", err)
	}
	defer func() {
		if p := recover(); p != nil { // 패닉 발생 시 롤백
			_ = tx.Rollback()
			panic(p) // 패닉 다시 던지기
		}
	}()

	// 쿼리 타임아웃 설정 (30초)
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	query := `
		INSERT INTO user_rankings (season_id, user_id, total_value, ranks, tier)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		total_value = VALUES(total_value),
		ranks = VALUES(ranks),
		tier = VALUES(tier)
	`

	stmt, err := tx.PrepareContext(queryCtx, query)
	if err != nil {
		_ = tx.Rollback() // 쿼리 준비 실패 시 롤백
		return fmt.Errorf("쿼리 준비 에러: %w", err)
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			log.Printf("쿼리 종료 에러: %v\n", err)
		}
	}()

	for _, userAsset := range userTotalAssets {
		_, err := stmt.ExecContext(queryCtx, seasonID, userAsset.UserID, userAsset.TotalValue, userAsset.Rank, userAsset.Tier)
		if err != nil {
			_ = tx.Rollback() // 쿼리 실행 실패 시 롤백
			return fmt.Errorf("쿼리 실행 에러: %w", err)
		}
	}

	// 모든 작업이 성공적으로 완료되었으므로 커밋 시도
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("트랜잭션 커밋 에러: %w", err)
	}

	return nil
}
