package main

import (
	"Bitground-go/config"
	"Bitground-go/service"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

// 배포 시 주석 처리!
func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	obj := make(map[string]interface{})

	// 환경 변수에서 DB 연결 정보 가져오기
	obj["DB_HOST"] = os.Getenv("DB_HOST")
	obj["DB_USER"] = os.Getenv("DB_USER")
	obj["DB_PASSWORD"] = os.Getenv("DB_PASSWORD")
	obj["DB_NAME"] = os.Getenv("DB_NAME")
	obj["TYPE"] = os.Getenv("TYPE")
	obj["TEST_TIME"] = os.Getenv("TEST_TIME")

	Main(obj)

	os.Exit(0)

	//userIDs, err := getParticipatingUserIDs(db)
	//if err != nil {
	//	fmt.Printf("유저 ID 조회 실패: %v\n", err)
	//}
	//
	//fmt.Println("시즌 참여중인 유저 ID 목록:", userIDs)
	//
	//coinPrices, err := getAllCoinsCurrentPrice(db)
	//
	//fmt.Println("모든 코인의 현재가:", coinPrices)
	//log.Println("프로그램 종료")
}

func Main(obj map[string]interface{}) map[string]interface{} {
	cfg := config.NewDBConfig(obj)

	// 데이터베이스 연결
	db, err := config.ConnectDB(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Println("연결 성공!")

	// Service 초기화 (단순화됨)
	testService := service.NewTestService(db)

	fmt.Println("\n--- 고정 데이터 삽입 테스트 ---")
	if err := testService.Insert(); err != nil {
		fmt.Printf("삽입 실패: %v\n", err)
	}

	log.Println("프로그램 종료")

	msg := make(map[string]interface{})
	msg["message"] = "success"
	return msg
}
