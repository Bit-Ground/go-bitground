package config

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

// DBConfig db 구성 구조체
type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	Charset  string
}

// NewDBConfig DBConfig 생성 함수
func NewDBConfig(obj map[string]interface{}) DBConfig {
	return DBConfig{
		Host:     obj["DB_HOST"].(string),
		User:     obj["DB_USER"].(string),
		Password: obj["DB_PASSWORD"].(string),
		DBName:   obj["DB_NAME"].(string),
		Charset:  "utf8mb4",
	}
}

// ConnectDB 데이터베이스 연결
func ConnectDB(ctx context.Context, cfg DBConfig) (*sql.DB, error) {

	// MySQL DSN 구성
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s&parseTime=true",
		cfg.User, cfg.Password, cfg.Host, cfg.DBName, cfg.Charset)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("데이터베이스 연결 실패: %v", err)
	}

	// Context를 활용한 연결 테스트 (타임아웃: 10초)
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 연결 테스트
	if err := db.PingContext(pingCtx); err != nil {
		if err := db.Close(); err != nil {
			return nil, fmt.Errorf("데이터베이스 연결 종료 실패: %v", err)
		}
		return nil, fmt.Errorf("데이터베이스 연결 확인 실패: %v", err)
	}

	// 커넥션 풀 설정
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	return db, nil
}
