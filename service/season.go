package service

import (
	"Bitground-go/model"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func UpdateSeason(ctx context.Context, db *sql.DB, seasonID int, coinPrices map[int]float64, obj map[string]interface{}) error {
	seasonName := obj["SEASON_NAME"].(string)
	chkType := obj["TYPE"].(string)
	seasonUpdateKey := obj["SEASON_UPDATE_KEY"].(string)

	// errgroup.WithContext는 컨텍스트와 함께 새로운 Group을 생성합니다.
	// Group 내의 고루틴 중 하나라도 에러를 반환하면, Group의 Context는 취소되고
	// Wait()는 첫 번째 에러를 반환합니다.
	g, gCtx := errgroup.WithContext(ctx)

	// 고루틴 시작 (gCtx를 사용)
	g.Go(func() error {
		return seasonClose(gCtx, db, seasonID, seasonName, chkType)
	})
	g.Go(func() error {
		return updateUserTiers(gCtx, db, seasonID)
	})
	g.Go(func() error {
		return resetUserCash(gCtx, db)
	})
	g.Go(func() error {
		return resetUserAssetsWithProgress(gCtx, db, seasonID, coinPrices)
	})
	g.Go(func() error { return deletePendingOrders(gCtx, db, seasonID) })

	// 모든 고루틴이 완료되거나, 첫 번째 에러가 발생할 때까지 대기
	// 에러가 발생하면 해당 에러를 반환합니다.
	if err := g.Wait(); err != nil {
		return fmt.Errorf("고루틴 수행 중 에러 발생: %w", err)
	}

	// 기존 시즌의 reward_calculated 컬럼을 업데이트
	if err := updateSeasonRewardCalculated(ctx, db, seasonID); err != nil {
		return fmt.Errorf("reward_calculated 업데이트 실패: %w", err)
	}

	// NotifySeasonUpdate 함수를 호출하여 스프링 서버에 시즌 업데이트 요청
	if err := NotifySeasonUpdate(ctx, seasonUpdateKey, "season"); err != nil {
		return fmt.Errorf("NotifySeasonUpdate 에러: %w", err)
	}

	return nil
}

// 기존 시즌 종료, 새 시즌 시작
func seasonClose(ctx context.Context, db *sql.DB, seasonID int, seasonName, chkType string) error {
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

	selectQuery := `SELECT name FROM seasons WHERE id = ?;`

	var ogSeasonName string
	err = tx.QueryRowContext(queryCtx, selectQuery, seasonID).Scan(&ogSeasonName)
	if errors.Is(err, sql.ErrNoRows) {
		ogSeasonName = "초기화 시즌 1"
	} else if err != nil {
		return fmt.Errorf("시즌 이름 조회 실패: %w", err)
	}

	// processSeasonStrings 함수를 호출하여 새로운 시즌 이름을 생성
	newSeasonName, err := processSeasonStrings(seasonName, ogSeasonName)
	if err != nil {
		return fmt.Errorf("시즌 이름 처리 실패: %w", err)
	}

	// 기존 시즌 종료 처리 쿼리
	updateQuery := `
		UPDATE seasons
		SET status = 'COMPLETED'
		WHERE id = ?;
	`

	_, err = tx.ExecContext(queryCtx, updateQuery, seasonID)
	if err != nil {
		return fmt.Errorf("시즌 상태 업데이트 실패: %w", err)
	}

	// 오늘 날짜 가져오기
	startAt := time.Now()
	var endAt time.Time
	if chkType == "dev" {
		endAt = startAt
	} else {
		if startAt.Day() == 1 {
			// 이번 달 15일로 종료일 설정
			endAt = time.Now().AddDate(0, 0, 14)
		} else if startAt.Day() == 16 {
			// 이번 달 마지막 날로 종료일 설정
			firstOfNextMonth := time.Date(startAt.Year(), startAt.Month()+1, 1, 0, 0, 0, 0, startAt.Location())
			endAt = firstOfNextMonth.AddDate(0, 0, -1)
		} else {
			return fmt.Errorf("시즌 종료일 설정 실패: 현재 날짜가 시즌 종료일이 아님")
		}
	}

	// 새 시즌 시작 처리 쿼리
	insertQuery := `
		INSERT INTO seasons (name, start_at, end_at) 
		VALUES (?, ?, ?);
	`

	_, err = tx.ExecContext(queryCtx, insertQuery, newSeasonName, startAt.Format("2006-01-02"), endAt.Format("2006-01-02"))
	if err != nil {
		return fmt.Errorf("새 시즌 생성 실패: %w", err)
	}

	return tx.Commit() // 트랜잭션 커밋
}

// processSeasonStrings 함수는 문자열 a와 b를 비교하여 새로운 문자열 c를 반환합니다.
func processSeasonStrings(seasonName, ogSeasonName string) (string, error) {
	// "시즌" 앞의 문자열과 "시즌" 자체를 캡쳐하는 정규 표현식 (예: "### 시즌", "시즌")
	reSeasonPrefix := regexp.MustCompile(`^(.*시즌)`)
	// "시즌 " 뒤의 숫자를 캡쳐하는 정규 표현식
	reNumber := regexp.MustCompile(`시즌 (\d+)$`)

	// 문자열 a에서 "시즌" 부분을 포함한 앞부분을 추출합니다.
	matchesNewPrefix := reSeasonPrefix.FindStringSubmatch(seasonName)
	var seasonPrefixNew string
	if len(matchesNewPrefix) > 1 {
		seasonPrefixNew = matchesNewPrefix[1] // 예: "### 시즌" 또는 "시즌"
	} else {
		return "", fmt.Errorf("문자열 '%s'에서 '시즌' 부분을 찾을 수 없습니다", seasonName)
	}

	// 문자열 b에서 "시즌" 부분을 포함한 앞부분을 추출합니다.
	matchesOgPrefix := reSeasonPrefix.FindStringSubmatch(ogSeasonName)
	var seasonPrefixOg string
	if len(matchesOgPrefix) > 1 {
		seasonPrefixOg = matchesOgPrefix[1] // 예: "~~~ 시즌" 또는 "시즌"
	} else {
		return "", fmt.Errorf("문자열 '%s'에서 '시즌' 부분을 찾을 수 없습니다", ogSeasonName)
	}

	// 문자열 b에서 숫자를 추출합니다.
	matchesOgNumber := reNumber.FindStringSubmatch(ogSeasonName)
	var numberOg int
	if len(matchesOgNumber) > 1 {
		num, err := strconv.Atoi(matchesOgNumber[1])
		if err != nil {
			return "", fmt.Errorf("문자열 '%s'에서 숫자를 파싱할 수 없습니다: %w", ogSeasonName, err)
		}
		numberOg = num
	} else {
		// 숫자가 없는 경우 (예: "시즌"만 있는 경우) 기본값을 0으로 설정하여 로직에서 1이 되도록 함
		numberOg = 0
	}

	// 문자열 a의 "### 시즌" 부분과 문자열 b의 "~~~ 시즌" 부분을 비교합니다.
	// 즉, '시즌'을 포함한 앞부분이 완전히 같은지 비교합니다.
	if seasonPrefixNew == seasonPrefixOg {
		// 같다면 ** 값을 1 증가시킵니다.
		numberOg++
	} else {
		// 다르다면 ** 값을 1로 만듭니다.
		numberOg = 1
	}

	// 문자열 c를 "### 시즌 **" 양식으로 출력합니다.
	// 이때, 문자열 c의 "### 시즌" 부분은 문자열 a에서 추출한 seasonPrefixA를 사용합니다.
	c := fmt.Sprintf("%s %d", seasonPrefixNew, numberOg)
	return c, nil
}

// 티어 users에 반영
func updateUserTiers(ctx context.Context, db *sql.DB, seasonID int) error {
	// 쿼리 타임아웃 설정 (20초)
	queryCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	// 티어 업데이트 쿼리
	updateQuery := `
		UPDATE users u
		LEFT JOIN user_rankings ur ON u.id = ur.user_id AND ur.season_id = ?
		SET u.tier = IFNULL(ur.tier, 0)
		WHERE u.is_deleted = 0;
	`

	_, err := db.ExecContext(queryCtx, updateQuery, seasonID)
	if err != nil {
		return fmt.Errorf("티어 업데이트 실패: %w", err)
	}

	return nil
}

// 캐시 초기화
func resetUserCash(ctx context.Context, db *sql.DB) error {
	// 쿼리 타임아웃 설정 (10초)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 캐시 초기화 쿼리
	updateQuery := `
		UPDATE users
		SET cash = 10000000
		WHERE is_deleted = 0;
	`
	_, err := db.ExecContext(queryCtx, updateQuery)
	if err != nil {
		return fmt.Errorf("캐시 초기화 실패: %w", err)
	}

	return nil
}

// user_assets 테이블 초기화
func resetUserAssets(ctx context.Context, db *sql.DB, seasonID int, coinPrices map[int]float64) error {
	// 1. user_assets 데이터를 orders로 이동
	err := migrateUserAssetsToOrders(ctx, db, seasonID, coinPrices)
	if err != nil {
		return fmt.Errorf("user_assets 데이터 이동 실패: %w", err)
	}

	// 2. user_assets 테이블 초기화
	// 쿼리 타임아웃 설정 (5초)
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// user_assets 테이블 초기화 쿼리
	deleteQuery := `
		DELETE FROM user_assets;
	`
	_, err = db.ExecContext(queryCtx, deleteQuery)
	if err != nil {
		return fmt.Errorf("user_assets 테이블 초기화 실패: %w", err)
	}

	return nil
}

// 기존 시즌 거래내역의 예약주문 내역들을 삭제합니다.
func deletePendingOrders(ctx context.Context, db *sql.DB, seasonID int) error {
	// 쿼리 타임아웃 설정 (10초)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 예약주문 내역 삭제 쿼리
	deleteQuery := `
		DELETE FROM orders
		WHERE season_id = ?
		AND status = 'PENDING';
	`

	_, err := db.ExecContext(queryCtx, deleteQuery, seasonID)
	if err != nil {
		return fmt.Errorf("예약주문 내역 삭제 실패: %w", err)
	}

	return nil
}

// 기존 시즌 reward_calculated 컬럼 수정
func updateSeasonRewardCalculated(ctx context.Context, db *sql.DB, seasonID int) error {
	// 쿼리 타임아웃 설정 (10초)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// reward_calculated 컬럼 업데이트 쿼리
	updateQuery := `
		UPDATE seasons
		SET reward_calculated = 1
		WHERE id = ?;
	`

	_, err := db.ExecContext(queryCtx, updateQuery, seasonID)
	if err != nil {
		return fmt.Errorf("시즌 reward_calculated 업데이트 실패: %w", err)
	}

	return nil
}

// NotifySeasonUpdate 내 스프링 서버에 시즌/스플릿 업데이트 요청을 보냅니다.
func NotifySeasonUpdate(ctx context.Context, seasonUpdateKey string, seasonFlag string) error {
	apiURL := "https://api.bitground.kr/seasons/update"

	// Context를 활용한 HTTP 요청 (타임아웃: 10초)
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// POST 요청을 위한 폼 데이터 생성
	formData := fmt.Sprintf("secretKey=%s&seasonFlag=%s", seasonUpdateKey, seasonFlag)

	req, err := http.NewRequestWithContext(reqCtx, "POST", apiURL, strings.NewReader(formData))
	if err != nil {
		return fmt.Errorf("HTTP 요청 생성 에러: %w", err)
	}

	// Content-Type 헤더 설정
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// 요청 보내기
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("API 요청 에러: %w", err)
	}
	defer func(Body io.ReadCloser) {
		// 응답 본문을 닫아 리소스 누수 방지
		if err := Body.Close(); err != nil {
			log.Printf("응답 본문 닫기 에러: %v\n", err)
		}
	}(resp.Body)

	// 응답 확인
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("서버 응답 에러: 상태 코드 %d", resp.StatusCode)
	}

	return nil
}

// user_assets 데이터를 orders 테이블로 배치 이동
func migrateUserAssetsToOrders(ctx context.Context, db *sql.DB, seasonID int, coinPrices map[int]float64) error {
	const batchSize = 1000
	offset := 0

	for {
		// 배치 단위로 user_assets 데이터 조회
		userAssets, err := fetchUserAssetsBatch(ctx, db, batchSize, offset)
		if err != nil {
			return fmt.Errorf("user_assets 배치 조회 실패 (offset: %d): %w", offset, err)
		}

		// 더 이상 데이터가 없으면 종료
		if len(userAssets) == 0 {
			break
		}

		// 배치 단위로 orders 테이블에 삽입
		err = insertOrdersBatch(ctx, db, userAssets, seasonID, coinPrices)
		if err != nil {
			return fmt.Errorf("orders 배치 삽입 실패 (offset: %d): %w", offset, err)
		}

		log.Printf("배치 처리 완료: %d개 레코드 처리됨 (offset: %d)\n", len(userAssets), offset)

		// 다음 배치로 이동
		offset += batchSize

		// 배치 처리 간 잠시 대기 (DB 부하 방지)
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

// user_assets에서 배치 단위로 데이터 조회
func fetchUserAssetsBatch(ctx context.Context, db *sql.DB, limit, offset int) ([]model.UserAsset, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `
		SELECT amount, symbol_id, user_id 
		FROM user_assets 
		ORDER BY id 
		LIMIT ? OFFSET ?
	`

	rows, err := db.QueryContext(queryCtx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			log.Printf("행 닫기 에러: %v\n", err)
		}
	}(rows)

	var userAssets []model.UserAsset
	for rows.Next() {
		var asset model.UserAsset
		err := rows.Scan(&asset.Amount, &asset.SymbolID, &asset.UserID)
		if err != nil {
			return nil, err
		}
		userAssets = append(userAssets, asset)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return userAssets, nil
}

// orders 테이블에 배치 삽입
func insertOrdersBatch(ctx context.Context, db *sql.DB, userAssets []model.UserAsset, seasonID int, coinPrices map[int]float64) error {
	if len(userAssets) == 0 {
		return nil
	}

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

	// bulk insert 쿼리 생성을 위한 준비
	valueStrings := make([]string, 0, len(userAssets))
	valueArgs := make([]interface{}, 0, len(userAssets)*5)

	for _, asset := range userAssets {
		// coinPrices 맵에서 trade_price 조회
		tradePrice, exists := coinPrices[asset.SymbolID]
		if !exists {
			// 가격 정보가 없는 경우 기본값 설정
			log.Printf("경고: symbol_id %d에 대한 가격 정보가 없습니다\n", asset.SymbolID)
			continue
		}

		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, 'SELL', 'COMPLETED')")
		valueArgs = append(valueArgs,
			asset.Amount,
			asset.SymbolID,
			asset.UserID,
			seasonID,
			tradePrice)
	}

	if len(valueStrings) == 0 {
		return nil // 삽입할 데이터가 없음
	}

	// bulk insert 쿼리 완성
	stmt := fmt.Sprintf(`
		INSERT INTO orders (amount, symbol_id, user_id, season_id, trade_price, order_type, status) 
		VALUES %s`, strings.Join(valueStrings, ","))

	_, err = tx.ExecContext(queryCtx, stmt, valueArgs...)
	if err != nil {
		return fmt.Errorf("orders 배치 삽입 실패: %w", err)
	}

	return tx.Commit()
}

// 전체 user_assets 개수 조회 (진행률 표시용)
func getUserAssetsCount(ctx context.Context, db *sql.DB) (int, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var count int
	query := `SELECT COUNT(*) FROM user_assets`
	err := db.QueryRowContext(queryCtx, query).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// 진행률과 함께 실행하는 래퍼 함수
func resetUserAssetsWithProgress(ctx context.Context, db *sql.DB, seasonID int, coinPrices map[int]float64) error {
	// 전체 개수 조회
	totalCount, err := getUserAssetsCount(ctx, db)
	if err != nil {
		return fmt.Errorf("user_assets 개수 조회 실패: %w", err)
	}

	log.Printf("총 %d개의 user_assets 레코드를 처리합니다\n", totalCount)

	// 초기화 실행
	err = resetUserAssets(ctx, db, seasonID, coinPrices)
	if err != nil {
		return err
	}

	log.Println("user_assets 테이블 초기화가 완료되었습니다")
	return nil
}
