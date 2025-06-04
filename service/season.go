package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"regexp"
	"strconv"
	"time"
)

func UpdateSeason(ctx context.Context, db *sql.DB, seasonID int, obj map[string]interface{}) error {
	seasonName := obj["SEASON_NAME"].(string)
	chkType := obj["TYPE"].(string)

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
		return resetUserAssets(gCtx, db)
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
		JOIN user_rankings ur ON u.id = ur.user_id
		SET u.tier = ur.tier
		WHERE ur.season_id = ?;
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
func resetUserAssets(ctx context.Context, db *sql.DB) error {
	// 쿼리 타임아웃 설정 (5초)
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// user_assets 테이블 초기화 쿼리
	deleteQuery := `
		DELETE FROM user_assets;
	`
	_, err := db.ExecContext(queryCtx, deleteQuery)
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
