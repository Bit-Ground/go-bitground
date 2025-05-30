package main

import (
	"Bitground-go/config"
	"Bitground-go/service"
	"Bitground-go/util"
	"context"
	"database/sql"
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"os"
	"time"
)

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
	obj["GOOGLE_API_KEY"] = os.Getenv("GOOGLE_API_KEY")

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
	// 전체 함수에 대한 타임아웃 설정 (서버리스 함수 제한시간보다 짧게)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg := config.NewDBConfig(obj)

	// 데이터베이스 연결
	db, err := config.ConnectDB(ctx, cfg)
	if err != nil {
		return makeMessage(fmt.Sprintf("데이터베이스 연결 실패: %v", err))
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	// 시간 설정
	flags, err := util.TimeCheck(obj)
	if err != nil {
		return makeMessage(fmt.Sprintf("시간 확인 실패: %v", err))
	}

	if flags.Split {
		fmt.Println("스플릿 업데이트 수행")
	} else {
		fmt.Println("스플릿 업데이트 미수행")
	}

	// 마켓 인덱스 업데이트 비동기로
	//marketIndexDone := make(chan error, 1)
	//go func() {
	//	log.Println("마켓 인덱스 업데이트 시작")
	//	err := service.UpdateMarketIndex(ctx, db)
	//	if err != nil {
	//		log.Printf("마켓 인덱스 업데이트 실패: %v\n", err)
	//	}
	//	marketIndexDone <- err
	//}()

	// api 키 설정
	geminiKey := obj["GOOGLE_API_KEY"].(string)
	// 인사이트 업데이트 비동기로
	insightDone := make(chan error, 1)
	go func() {
		log.Println("인사이트 업데이트 시작")
		err := service.UpdateInsight(ctx, db, geminiKey)
		if err != nil {
			log.Printf("인사이트 업데이트 실패: %v\n", err)
		}
		insightDone <- err
	}()

	// 마켓 인덱스 완료 대기
	//if err := <-marketIndexDone; err != nil {
	//	return makeMessage(fmt.Sprintf("마켓 인덱스 업데이트 실패: %v", err))
	//}

	// 인사이트 업데이트 완료 대기
	if err := <-insightDone; err != nil {
		return makeMessage(fmt.Sprintf("인사이트 업데이트 실패: %v", err))
	}

	// 메시지 생성
	return makeMessage("success")
}

func makeMessage(msg string) map[string]interface{} {
	message := make(map[string]interface{})
	message["message"] = msg
	return message
}
