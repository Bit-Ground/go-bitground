package service

import (
	"database/sql"
	"fmt"
)

type TestService struct {
	db *sql.DB
}

func NewTestService(db *sql.DB) *TestService {
	return &TestService{db: db}
}

// Insert insert test
func (s *TestService) Insert() error {
	// 트랜잭션 시작
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("트랜잭션 시작 실패: %v", err)
	}
	defer tx.Rollback()

	// INSERT 쿼리 실행
	insertQuery := `INSERT INTO notifications(created_at, fcm_token, updated_at, user_id) 
                   VALUES ('2023-10-01 12:00:00', 'fcm_token_123', '2023-10-01 12:00:00', 1)`

	result, err := tx.Exec(insertQuery)
	if err != nil {
		return fmt.Errorf("INSERT 실행 실패: %v", err)
	}

	// 삽입된 행의 개수 확인
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("영향받은 행 수 확인 실패: %v", err)
	}

	// 마지막 삽입된 ID 확인 (AUTO_INCREMENT 컬럼이 있는 경우)
	lastInsertID, err := result.LastInsertId()
	if err != nil {
		fmt.Printf("마지막 삽입 ID 확인 실패: %v\n", err)
	} else {
		fmt.Printf("삽입된 레코드 ID: %d\n", lastInsertID)
	}

	fmt.Printf("삽입된 행 개수: %d\n", rowsAffected)

	// 트랜잭션 커밋
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("트랜잭션 커밋 실패: %v", err)
	}

	fmt.Println("데이터 삽입 성공!")
	return nil
}
