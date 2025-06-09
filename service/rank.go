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

const BATCH_SIZE = 1000 // 배치 크기 설정

func UpdateRank(ctx context.Context, db *sql.DB, symbolMap map[string]int, seasonID int) error {
	// 1. 총 참여 유저 수 확인
	totalUsers, err := getParticipatingUserCount(ctx, db, seasonID)
	if err != nil {
		return fmt.Errorf("유저 수 조회 실패: %w", err)
	}

	if totalUsers == 0 {
		return nil
	}

	// 2. 코인 현재가 가져오기
	coinPrices, err := getAllCoinsCurrentPrice(ctx, symbolMap)
	if err != nil {
		return fmt.Errorf("코인 현재가 조회 실패: %w", err)
	}

	// 3. 배치 처리로 랭킹 업데이트
	return updateRankWithBatching(ctx, db, seasonID, coinPrices, totalUsers)
}

// 배치 처리 방식 (모든 경우에 사용)
func updateRankWithBatching(ctx context.Context, db *sql.DB, seasonID int, coinPrices map[int]float64, totalUsers int) error {
	// 1. 임시 테이블 생성
	if err := createTempRankingTable(ctx, db); err != nil {
		return fmt.Errorf("임시 테이블 생성 실패: %w", err)
	}
	defer dropTempRankingTable(ctx, db) // 정리

	// 2. 배치 단위로 유저 자산 계산 및 임시 테이블에 저장
	offset := 0
	for offset < totalUsers {
		if err := processBatch(ctx, db, seasonID, coinPrices, offset, BATCH_SIZE); err != nil {
			return fmt.Errorf("배치 처리 실패 (offset: %d): %w", offset, err)
		}
		offset += BATCH_SIZE

		// 메모리 정리를 위한 가비지 컬렉션 힌트
		if offset%(BATCH_SIZE*5) == 0 {
			log.Printf("처리 진행률: %d/%d", offset, totalUsers)
		}
	}

	// 3. 임시 테이블에서 랭킹 계산 및 업데이트
	if err := finalizeRankingFromTemp(ctx, db, seasonID, totalUsers); err != nil {
		return fmt.Errorf("임시 테이블에서 랭킹 계산 실패: %w", err)
	}

	return nil
}

// 참여 유저 수 조회
func getParticipatingUserCount(ctx context.Context, db *sql.DB, seasonID int) (int, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `
		SELECT COUNT(DISTINCT user_id)
		FROM orders
		WHERE season_id = ? AND status = 'COMPLETED'
	`

	var count int
	err := db.QueryRowContext(queryCtx, query, seasonID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("쿼리 실행 에러: %w", err)
	}

	return count, nil
}

// 임시 랭킹 테이블 생성
func createTempRankingTable(ctx context.Context, db *sql.DB) error {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 기존 테이블이 있으면 제거
	dropQuery := `DROP TABLE IF EXISTS temp_user_rankings`
	_, err := db.ExecContext(queryCtx, dropQuery)
	if err != nil {
		return fmt.Errorf("기존 임시 테이블 제거 실패: %w", err)
	}

	// 일반 테이블로 생성 (TEMPORARY 키워드 제거)
	createQuery := `
        CREATE TABLE temp_user_rankings (
            user_id INT PRIMARY KEY,
            total_value BIGINT NOT NULL,
            INDEX idx_total_value (total_value DESC)
        )
    `

	_, err = db.ExecContext(queryCtx, createQuery)
	return err
}

// 배치 단위로 유저 자산 계산
func processBatch(ctx context.Context, db *sql.DB, seasonID int, coinPrices map[int]float64, offset, limit int) error {
	// 1. 배치 단위로 유저 ID 가져오기
	userIDs, err := getParticipatingUserIDsBatch(ctx, db, seasonID, offset, limit)
	if err != nil {
		return err
	}

	if len(userIDs) == 0 {
		return nil
	}

	// 2. 해당 유저들의 현금 및 자산 정보 가져오기
	g, gCtx := errgroup.WithContext(ctx)
	var userCashMap map[int]int
	var userAssetsMap map[int][]model.UserAsset

	g.Go(func() error {
		cash, err := getUserCashMap(gCtx, db, userIDs)
		userCashMap = cash
		return err
	})

	g.Go(func() error {
		assets, err := getUserAssetsMap(gCtx, db, userIDs)
		userAssetsMap = assets
		return err
	})

	if err := g.Wait(); err != nil {
		return err
	}

	// 3. 총 자산 계산 후 임시 테이블에 저장
	return insertBatchToTemp(ctx, db, userIDs, userCashMap, userAssetsMap, coinPrices)
}

// 배치 단위로 유저 ID 조회
func getParticipatingUserIDsBatch(ctx context.Context, db *sql.DB, seasonID, offset, limit int) ([]int, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `
		SELECT DISTINCT user_id
		FROM orders
		WHERE season_id = ? AND status = 'COMPLETED'
		ORDER BY user_id
		LIMIT ? OFFSET ?
	`

	rows, err := db.QueryContext(queryCtx, query, seasonID, limit, offset)
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

	return userIDs, rows.Err()
}

// 배치를 임시 테이블에 저장
func insertBatchToTemp(ctx context.Context, db *sql.DB, userIDs []int, userCashMap map[int]int, userAssetsMap map[int][]model.UserAsset, coinPrices map[int]float64) error {
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	tx, err := db.BeginTx(queryCtx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		} else if err != nil {
			_ = tx.Rollback()
		}
	}()

	query := `INSERT INTO temp_user_rankings (user_id, total_value) VALUES (?, ?)`
	stmt, err := tx.PrepareContext(queryCtx, query)
	if err != nil {
		return fmt.Errorf("쿼리 준비 에러: %w", err)
	}
	defer func(stmt *sql.Stmt) {
		if err := stmt.Close(); err != nil {
			log.Printf("쿼리 종료 에러: %v\n", err)
		}
	}(stmt)

	for _, userID := range userIDs {
		totalValue := calculateUserTotalValue(userID, userCashMap, userAssetsMap, coinPrices)
		if _, err := stmt.ExecContext(queryCtx, userID, totalValue); err != nil {
			return fmt.Errorf("임시 테이블에 데이터 삽입 실패 (user_id: %d): %w", userID, err)
		}

	}

	return tx.Commit()
}

// 임시 테이블에서 최종 랭킹 계산 및 업데이트
func finalizeRankingFromTemp(ctx context.Context, db *sql.DB, seasonID, totalUsers int) error {
	queryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	// CTE로 rank를 한 번만 계산하고 tier를 결정
	query := `
		INSERT INTO user_rankings (season_id, user_id, total_value, ranks, tier)
		SELECT 
		   ? as season_id,
		   user_id,
		   total_value,
		   ranks,
		   CASE 
			  WHEN ranks = 1 THEN 7
			  WHEN ranks <= 3 THEN 6
			  WHEN (ranks - 3) / GREATEST(? - 3, 1.0) <= 0.10 THEN 5
			  WHEN (ranks - 3) / GREATEST(? - 3, 1.0) <= 0.25 THEN 4
			  WHEN (ranks - 3) / GREATEST(? - 3, 1.0) <= 0.45 THEN 3
			  WHEN (ranks - 3) / GREATEST(? - 3, 1.0) <= 0.70 THEN 2
			  ELSE 1
		   END as tier
		FROM (
		   SELECT 
			  user_id,
			  total_value,
			  ROW_NUMBER() OVER (ORDER BY total_value DESC) as ranks
		   FROM temp_user_rankings
		) AS ranked_users
		ORDER BY ranks
		ON DUPLICATE KEY UPDATE
		   total_value = VALUES(total_value),
		   ranks = VALUES(ranks),
		   tier = VALUES(tier)
	`

	_, err := db.ExecContext(queryCtx, query, seasonID, totalUsers, totalUsers, totalUsers, totalUsers)
	return err
}

// 임시 테이블 정리
func dropTempRankingTable(ctx context.Context, db *sql.DB) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := db.ExecContext(queryCtx, "DROP TABLE IF EXISTS temp_user_rankings")
	if err != nil {
		log.Printf("임시 테이블 삭제 실패: %v", err)
	}
}

// getParticipatingUserIDs - 더 이상 사용하지 않지만 하위 호환성을 위해 남겨둠
// 배치 처리에서는 getParticipatingUserIDsBatch를 사용

func getAllCoinsCurrentPrice(ctx context.Context, symbolMap map[string]int) (map[int]float64, error) {
	apiURL := "https://api.upbit.com/v1/ticker/all?quote_currencies=KRW"

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("HTTP 요청 생성 에러: %w", err)
	}

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

	var upbitPrices []model.UpbitCoinPrice
	err = json.NewDecoder(resp.Body).Decode(&upbitPrices)
	if err != nil {
		return nil, fmt.Errorf("JSON 디코딩 에러: %w", err)
	}

	coinPrices := make(map[int]float64)
	for _, upbitCoin := range upbitPrices {
		symbol := upbitCoin.Market
		if symbolId, exists := symbolMap[symbol]; exists {
			coinPrices[symbolId] = upbitCoin.TradePrice
		}
	}

	return coinPrices, nil
}

func getUserCashMap(ctx context.Context, db *sql.DB, userIDs []int) (map[int]int, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

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

func getUserAssetsMap(ctx context.Context, db *sql.DB, userIDs []int) (map[int][]model.UserAsset, error) {
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

func calculateUserTotalValue(userID int, userCashMap map[int]int, userAssetsMap map[int][]model.UserAsset, priceMap map[int]float64) int {
	cash := userCashMap[userID]
	totalValue := float64(cash)

	assets := userAssetsMap[userID]
	for _, asset := range assets {
		if price, exists := priceMap[asset.SymbolID]; exists {
			totalValue += asset.Amount * price
		}
	}

	return int(totalValue)
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
