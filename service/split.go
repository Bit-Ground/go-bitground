package service

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func UpdateSplit(ctx context.Context, db *sql.DB, obj map[string]interface{}) error {
	seasonUpdateKey := obj["SEASON_UPDATE_KEY"].(string)
	// db에서 모든 탈퇴하지 않은 유저의 자산을 천만 씩 추가

	// 쿼리 타임아웃 설정 (10초)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `
		UPDATE users
		SET cash = cash + 10000000
		WHERE is_deleted = 0;
	`

	_, err := db.ExecContext(queryCtx, query)
	if err != nil {
		return fmt.Errorf("쿼리 실행 에러: %w", err)
	}

	// NotifySeasonUpdate 함수를 호출하여 스프링 서버에 시즌 업데이트 요청
	if err := NotifySeasonUpdate(ctx, seasonUpdateKey, "split"); err != nil {
		return fmt.Errorf("NotifySeasonUpdate 에러: %w", err)
	}

	return nil
}
