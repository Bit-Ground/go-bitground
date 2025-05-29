package config

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
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
func ConnectDB(cfg DBConfig) (*sql.DB, error) {

	// MySQL DSN 구성
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s&parseTime=true",
		cfg.User, cfg.Password, cfg.Host, cfg.DBName, cfg.Charset)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("데이터베이스 연결 실패: %v", err)
	}

	// 연결 테스트
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("데이터베이스 연결 확인 실패: %v", err)
	}

	return db, nil
}
