package main

import (
	"Bitground-go/config"
	"Bitground-go/service"
	"Bitground-go/util"
	"context"
	"database/sql"
	"github.com/joho/godotenv"
	"golang.org/x/sync/errgroup"
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
	obj["SEASON_NAME"] = os.Getenv("SEASON_NAME")
	obj["SEASON_UPDATE_KEY"] = os.Getenv("SEASON_UPDATE_KEY")

	Main(obj)
}

func Main(obj map[string]interface{}) map[string]interface{} {
	isSuccess := true

	// 전체 함수에 대한 타임아웃 설정 (서버리스 함수 제한시간보다 짧게)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	cfg := config.NewDBConfig(obj)

	// 1. 데이터베이스 연결
	db, err := config.ConnectDB(ctx, cfg)
	if err != nil {
		log.Println("데이터베이스 연결 실패:", err)
		return makeMessage("데이터베이스 연결 실패: " + err.Error())
	} else {
		log.Println("데이터베이스 연결 성공")
	}
	defer func(db *sql.DB) {
		if err := db.Close(); err != nil {
			log.Printf("데이터베이스 연결 종료 실패: %v\n", err)
		}
	}(db)

	// 2. 마켓 인덱스 업데이트 (비동기)
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		log.Println("마켓 인덱스 업데이트 시작")
		err := service.UpdateMarketIndex(gCtx, db)
		if err != nil {
			isSuccess = false
			log.Println("마켓 인덱스 업데이트 실패:", err)
		}
		log.Println("마켓 인덱스 업데이트 완료")
		return nil
	})

	// 3-1. db에서 코인 심볼 조회하여 심볼 맵 생성
	symbolMap, err := service.GetActiveCoinsSymbols(ctx, db)
	if err != nil {
		isSuccess = false
		log.Println("활성화된 코인 심볼 조회 실패:", err)
	}

	//3-2. 현 시즌 id 조회
	seasonID, err := service.GetCurrentSeasonID(ctx, db)
	if err != nil {
		isSuccess = false
		log.Println("현재 시즌 ID 조회 실패:", err)
	}

	// 4. 시간과 환경변수 통해 업데이트 플래그 확인
	flags, err := util.TimeCheck(obj)
	if err != nil {
		isSuccess = false
		log.Println("업데이트 플래그 확인 실패:", err)
	}

	// 5. 플래그에 따라 업데이트 수행
	log.Printf("업데이트 플래그: %+v\n", flags)

	// 5-1. 인사이트 업데이트 (비동기)
	g.Go(func() error {
		if flags.Insight {
			geminiKey := obj["GOOGLE_API_KEY"].(string)
			log.Println("인사이트 업데이트 시작")
			err := service.UpdateInsight(gCtx, db, geminiKey, symbolMap)
			if err != nil {
				isSuccess = false
				log.Println("인사이트 업데이트 실패:", err)
			} else {
				log.Println("인사이트 업데이트 완료")
			}
		} else {
			log.Println("인사이트 업데이트 생략")
		}
		return nil
	})

	// 5-2. 코인 업데이트 수행
	if flags.Coin {
		log.Println("코인 업데이트 시작")
		err = service.UpdateCoins(ctx, db)
		if err != nil {
			isSuccess = false
			log.Println("코인 업데이트 실패:", err)
		} else {
			log.Println("코인 업데이트 완료")
		}
	} else {
		log.Println("코인 업데이트 생략")
	}

	// 5-3. 유저 자산 업데이트 수행
	if flags.Split {
		log.Println("유저 자산 업데이트 시작")
		err = service.UpdateSplit(ctx, db, obj)
		if err != nil {
			isSuccess = false
			log.Println("유저 자산 업데이트 실패:", err)
		} else {
			log.Println("유저 자산 업데이트 완료")
		}
	} else {
		log.Println("유저 자산 업데이트 생략")
	}

	// 5-4. 랭킹 업데이트 수행
	log.Println("랭킹 업데이트 시작")
	if flags.Insight {
		log.Println("유저 자산 스냅샷 업데이트 시작")
	} else {
		log.Println("유저 자산 스냅샷 업데이트 생략")
	}
	err, coinPriceHistory, coinPrices := service.UpdateRank(ctx, db, symbolMap, seasonID, flags.Coin)
	if err != nil {
		isSuccess = false
		log.Println("랭킹(& 유저 자산 스냅샷) 업데이트 실패:", err)
	} else {
		log.Println("랭킹 업데이트 완료")
		if flags.Insight {
			log.Println("유저 자산 스냅샷 업데이트 완료")
		}
	}

	// 5-5. 시즌 업데이트 수행
	if flags.Season {
		log.Println("시즌 업데이트 시작")
		err = service.UpdateSeason(ctx, db, seasonID, coinPrices, obj)
		if err != nil {
			isSuccess = false
			log.Println("시즌 업데이트 실패:", err)
		} else {
			log.Println("시즌 업데이트 완료")
		}
	} else {
		log.Println("시즌 업데이트 생략")
	}

	// 6. (추가 요구사항) 코인 가격 히스토리 업데이트
	log.Println("코인 가격 히스토리 업데이트 시작")
	err = service.UpdateCoinPriceHistory(ctx, db, coinPriceHistory)
	if err != nil {
		isSuccess = false
		log.Println("코인 가격 히스토리 업데이트 실패:", err)
	} else {
		log.Println("코인 가격 히스토리 업데이트 완료")
	}

	// 종료 전 고루틴 대기
	if err := g.Wait(); err != nil {
		isSuccess = false
		log.Println("고루틴 수행 중 에러 발생:", err)
	}

	// 완료 여부 메시지 생성
	if isSuccess {
		return makeMessage("모든 업데이트 작업이 성공적으로 완료되었습니다.")
	} else {
		return makeMessage("업데이트 작업 중 일부가 실패했습니다.")
	}
}

func makeMessage(msg string) map[string]interface{} {
	// 로그 기록
	log.Println(msg)
	// 메시지 맵 생성
	message := make(map[string]interface{})
	message["message"] = msg
	return message
}
