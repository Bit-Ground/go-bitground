package service

import (
	"database/sql"
)

type RankUpdateService struct {
	db *sql.DB
}

func NewRankUpdateService(db *sql.DB) *RankUpdateService {
	return &RankUpdateService{db: db}
}

//// 시즌 참여중 userId 목록 가져오기
//func getParticipatingUserIDs(db *sql.DB) ([]int, error) {
//	query := `
//        SELECT DISTINCT o.user_id
//        FROM orders o
//        JOIN seasons s ON o.season_id = s.id
//        WHERE s.status = 'PENDING' AND o.status = 'COMPLETED'
//    `
//
//	rows, err := db.Query(query)
//	if err != nil {
//		return nil, err
//	}
//	defer rows.Close()
//
//	var userIDs []int
//
//	for rows.Next() {
//		var userID int
//		if err := rows.Scan(&userID); err != nil {
//			return nil, err
//		}
//		userIDs = append(userIDs, userID)
//	}
//
//	if err := rows.Err(); err != nil {
//		return nil, err
//	}
//
//	return userIDs, nil
//}
//
//// 데이터베이스에서 활성화된 코인 심볼들을 가져오는 함수
//func getActiveCoinsSymbols(db *sql.DB) (map[string]int, error) {
//	query := `
//        SELECT id, symbol
//        FROM coins
//        WHERE is_deleted = 0
//        ORDER BY symbol;
//    `
//
//	rows, err := db.Query(query)
//	if err != nil {
//		return nil, fmt.Errorf("쿼리 실행 에러: %v", err)
//	}
//	defer rows.Close()
//
//	symbolMap := make(map[string]int)
//	for rows.Next() {
//		var coinSymbol CoinSymbol
//		err := rows.Scan(&coinSymbol.Id, &coinSymbol.Symbol)
//		if err != nil {
//			return nil, fmt.Errorf("행 스캔 에러: %v", err)
//		}
//		symbolMap[coinSymbol.Symbol] = coinSymbol.Id
//	}
//
//	if err = rows.Err(); err != nil {
//		return nil, fmt.Errorf("행 반복 에러: %v", err)
//	}
//
//	return symbolMap, nil
//}
//
//// upbit API를 사용하여 모든 코인의 현재가를 가져오는 함수
//func getAllCoinsCurrentPrice(db *sql.DB) ([]CoinPrice, error) {
//	// 1. 데이터베이스에서 활성화된 코인 심볼 정보 가져오기
//	symbolMap, err := getActiveCoinsSymbols(db)
//	if err != nil {
//		return nil, fmt.Errorf("심볼 정보 조회 에러: %v", err)
//	}
//
//	// 2. 업비트 API 호출
//	resp, err := http.Get("https://api.upbit.com/v1/ticker/all?quote_currencies=KRW")
//	if err != nil {
//		return nil, fmt.Errorf("API 호출 에러: %v", err)
//	}
//	defer resp.Body.Close()
//
//	// 3. API 응답 디코딩
//	var upbitPrices []UpbitCoinPrice
//	err = json.NewDecoder(resp.Body).Decode(&upbitPrices)
//	if err != nil {
//		return nil, fmt.Errorf("JSON 디코딩 에러: %v", err)
//	}
//
//	// 4. 업비트 데이터와 데이터베이스 심볼 매칭
//	var coinPrices []CoinPrice
//	for _, upbitCoin := range upbitPrices {
//		// market에서 심볼 추출
//		symbol := upbitCoin.Market
//
//		// 데이터베이스에서 해당 심볼의 ID 찾기
//		if symbolId, exists := symbolMap[symbol]; exists {
//			coinPrice := CoinPrice{
//				SymbolId:   symbolId,
//				TradePrice: upbitCoin.TradePrice,
//			}
//			coinPrices = append(coinPrices, coinPrice)
//		}
//	}
//
//	// 5. SymbolId 순으로 정렬
//	sort.Slice(coinPrices, func(i, j int) bool {
//		return coinPrices[i].SymbolId < coinPrices[j].SymbolId
//	})
//
//	return coinPrices, nil
//}
//
//// 메인 정산 로직
//func calculateUserRankings(db *sql.DB, userIDs []int, coinPrices []CoinPrice, seasonID int) error {
//	// 1. 코인 가격을 맵으로 변환 (빠른 조회를 위해)
//	priceMap := make(map[int]float64)
//	for _, coin := range coinPrices {
//		priceMap[coin.SymbolId] = coin.TradePrice
//	}
//
//	// 2. 모든 유저의 현금 정보 조회
//	userCashMap, err := getUserCashMap(db, userIDs)
//	if err != nil {
//		return fmt.Errorf("failed to get user cash: %w", err)
//	}
//
//	// 3. 모든 유저의 자산 정보 조회
//	userAssetsMap, err := getUserAssetsMap(db, userIDs)
//	if err != nil {
//		return fmt.Errorf("failed to get user assets: %w", err)
//	}
//
//	// 4. 각 유저의 총 자산 계산
//	var userTotalAssets []UserTotalAsset
//	for _, userID := range userIDs {
//		totalValue := calculateUserTotalValue(userID, userCashMap, userAssetsMap, priceMap)
//		userTotalAssets = append(userTotalAssets, UserTotalAsset{
//			UserID:     userID,
//			TotalValue: totalValue,
//		})
//	}
//
//	// 5. 랭킹 계산 (총 자산 기준 내림차순 정렬)
//	sort.Slice(userTotalAssets, func(i, j int) bool {
//		return userTotalAssets[i].TotalValue > userTotalAssets[j].TotalValue
//	})
//
//	// 6. 랭크와 티어 할당
//	for i := range userTotalAssets {
//		userTotalAssets[i].Rank = i + 1
//		userTotalAssets[i].Tier = calculateTier(i+1, len(userTotalAssets))
//	}
//
//	// 7. 데이터베이스 업데이트
//	err = updateUserRankings(db, userTotalAssets, seasonID)
//	if err != nil {
//		return fmt.Errorf("failed to update user rankings: %w", err)
//	}
//
//	return nil
//}
//
//// 유저별 현금 정보 조회
//func getUserCashMap(db *sql.DB, userIDs []int) (map[int]int, error) {
//	if len(userIDs) == 0 {
//		return make(map[int]int), nil
//	}
//
//	// IN 절을 위한 placeholder 생성
//	placeholders := make([]interface{}, len(userIDs))
//	for i, id := range userIDs {
//		placeholders[i] = id
//	}
//
//	query := `SELECT id, cash FROM users WHERE id IN (` + generatePlaceholders(len(userIDs)) + `)`
//	rows, err := db.Query(query, placeholders...)
//	if err != nil {
//		return nil, err
//	}
//	defer rows.Close()
//
//	userCashMap := make(map[int]int)
//	for rows.Next() {
//		var userCash UserCash
//		err := rows.Scan(&userCash.UserID, &userCash.Cash)
//		if err != nil {
//			return nil, err
//		}
//		userCashMap[userCash.UserID] = userCash.Cash
//	}
//
//	return userCashMap, nil
//}
//
//// 유저별 자산 정보 조회
//func getUserAssetsMap(db *sql.DB, userIDs []int) (map[int][]UserAsset, error) {
//	if len(userIDs) == 0 {
//		return make(map[int][]UserAsset), nil
//	}
//
//	placeholders := make([]interface{}, len(userIDs))
//	for i, id := range userIDs {
//		placeholders[i] = id
//	}
//
//	query := `SELECT user_id, symbol_id, amount FROM user_assets WHERE user_id IN (` + generatePlaceholders(len(userIDs)) + `)`
//	rows, err := db.Query(query, placeholders...)
//	if err != nil {
//		return nil, err
//	}
//	defer rows.Close()
//
//	userAssetsMap := make(map[int][]UserAsset)
//	for rows.Next() {
//		var asset UserAsset
//		err := rows.Scan(&asset.UserID, &asset.SymbolID, &asset.Amount)
//		if err != nil {
//			return nil, err
//		}
//		userAssetsMap[asset.UserID] = append(userAssetsMap[asset.UserID], asset)
//	}
//
//	return userAssetsMap, nil
//}
//
//// 개별 유저의 총 자산 계산
//func calculateUserTotalValue(userID int, userCashMap map[int]int, userAssetsMap map[int][]UserAsset, priceMap map[int]float64) int {
//	// 현금
//	cash := userCashMap[userID]
//	totalValue := float64(cash)
//
//	// 코인 자산
//	assets := userAssetsMap[userID]
//	for _, asset := range assets {
//		if price, exists := priceMap[asset.SymbolID]; exists {
//			totalValue += asset.Amount * price
//		}
//	}
//
//	return int(totalValue)
//}
//
//// 티어 계산 (랭킹 기준으로 1~5 티어 부여)
//func calculateTier(rank, totalUsers int) int {
//	percentage := float64(rank) / float64(totalUsers)
//
//	switch {
//	case percentage <= 0.05: // 상위 5%
//		return 5
//	case percentage <= 0.15: // 상위 15%
//		return 4
//	case percentage <= 0.35: // 상위 35%
//		return 3
//	case percentage <= 0.65: // 상위 65%
//		return 2
//	default: // 나머지
//		return 1
//	}
//}
//
//// 유저 랭킹 정보 업데이트
//func updateUserRankings(db *sql.DB, userTotalAssets []UserTotalAsset, seasonID int) error {
//	query := `
//		INSERT INTO user_rankings (season_id, user_id, total_value, ranks, tier, created_at, updated_at)
//		VALUES (?, ?, ?, ?, ?, NOW(6), NOW(6))
//		ON DUPLICATE KEY UPDATE
//		total_value = VALUES(total_value),
//		ranks = VALUES(ranks),
//		tier = VALUES(tier),
//		updated_at = NOW(6)
//	`
//
//	stmt, err := db.Prepare(query)
//	if err != nil {
//		return err
//	}
//	defer stmt.Close()
//
//	for _, userAsset := range userTotalAssets {
//		_, err := stmt.Exec(seasonID, userAsset.UserID, userAsset.TotalValue, userAsset.Rank, userAsset.Tier)
//		if err != nil {
//			return fmt.Errorf("failed to update ranking for user %d: %w", userAsset.UserID, err)
//		}
//	}
//
//	return nil
//}
