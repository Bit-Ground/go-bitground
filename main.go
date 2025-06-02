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

// 배포 시 주석
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
}

func Main(obj map[string]interface{}) map[string]interface{} {
	// 전체 함수에 대한 타임아웃 설정 (서버리스 함수 제한시간보다 짧게)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg := config.NewDBConfig(obj)

	// 1. 데이터베이스 연결
	db, err := config.ConnectDB(ctx, cfg)
	if err != nil {
		return makeMessage(fmt.Sprintf("데이터베이스 연결 실패: %v", err))
	} else {
		log.Println("데이터베이스 연결 성공")
	}
	defer func(db *sql.DB) {
		if err := db.Close(); err != nil {
			log.Printf("데이터베이스 연결 종료 실패: %v\n", err)
		}
	}(db)

	// 2. 마켓 인덱스 업데이트 (비동기)
	marketIndexDone := make(chan error, 1)
	go func() {
		log.Println("마켓 인덱스 업데이트 시작")
		err := service.UpdateMarketIndex(ctx, db)
		if err != nil {
			log.Printf("마켓 인덱스 업데이트 실패: %v\n", err)
		}
		marketIndexDone <- err
	}()

	// 3-1. db에서 코인 심볼 조회하여 심볼 맵 생성
	symbolMap, err := service.GetActiveCoinsSymbols(ctx, db)
	if err != nil {
		return makeMessage(fmt.Sprintf("활성화된 코인 심볼 조회 실패: %v", err))
	}

	//3-2. 현 시즌 id 조회
	seasonID, err := service.GetCurrentSeasonID(ctx, db)
	if err != nil {
		return makeMessage(fmt.Sprintf("현재 시즌 ID 조회 실패: %v", err))
	}

	// 4. 시간과 환경변수 통해 업데이트 플래그 확인
	flags, err := util.TimeCheck(obj)
	if err != nil {
		return makeMessage(fmt.Sprintf("시간 확인 실패: %v", err))
	}

	// 5. 플래그에 따라 업데이트 수행
	log.Printf("업데이트 플래그: %+v\n", flags)

	// 5-1. 인사이트 업데이트 (비동기)
	geminiKey := obj["GOOGLE_API_KEY"].(string)
	insightDone := make(chan error, 1)
	go func() {
		var err error
		if flags.Insight {
			log.Println("인사이트 업데이트 시작")
			err = service.UpdateInsight(ctx, db, geminiKey, symbolMap)
			if err != nil {
				log.Printf("인사이트 업데이트 실패: %v\n", err)
			}
		} else {
			log.Println("인사이트 업데이트 생략")
		}
		insightDone <- err
	}()

	// 5-2. 코인 업데이트 수행
	if flags.Coin {
		log.Println("코인 업데이트 시작")
		err = service.UpdateCoins(ctx, db)
		if err != nil {
			return makeMessage(fmt.Sprintf("코인 업데이트 실패: %v", err))
		} else {
			log.Println("코인 업데이트 완료")
		}
	}

	// 5-3. 유저 자산 업데이트 수행
	if flags.Split {
		// 유저 자산 업데이트 수행
	}

	// 5-4. 랭킹 업데이트 수행
	log.Println("랭킹 업데이트 시작")
	err = service.UpdateRank(ctx, db, symbolMap, seasonID)
	if err != nil {
		return makeMessage(fmt.Sprintf("랭킹 업데이트 실패: %v", err))
	} else {
		log.Println("랭킹 업데이트 완료")
	}

	// 5-5. 시즌 업데이트 수행
	if flags.Season {
		// 시즌 업데이트 수행
	}

	// 종료 전 1. 마켓 인덱스 업데이트 완료 대기
	if err := <-marketIndexDone; err != nil {
		return makeMessage(fmt.Sprintf("마켓 인덱스 업데이트 실패: %v", err))
	} else {
		log.Println("마켓 인덱스 업데이트 완료")
	}

	// 종료 전 2. 인사이트 업데이트 완료 대기
	if err := <-insightDone; err != nil {
		return makeMessage(fmt.Sprintf("인사이트 업데이트 실패: %v", err))
	} else {
		log.Println("인사이트 업데이트 완료")
	}

	// 성공 메시지 생성
	return makeMessage("success")
}

func makeMessage(msg string) map[string]interface{} {
	// 로그 기록
	log.Println(msg)
	// 메시지 맵 생성
	message := make(map[string]interface{})
	message["message"] = msg
	return message
}
