package util

import (
	"Bitground-go/model"
	"fmt"
	"time"
)

// GeneratePlaceholders IN 절 placeholder 생성 헬퍼 함수
func GeneratePlaceholders(count int) string {
	if count == 0 {
		return ""
	}

	placeholders := "?"
	for i := 1; i < count; i++ {
		placeholders += ",?"
	}
	return placeholders
}

// TimeCheck 시간 확인하여 수행할 업데이트 플래그를 반환하는 함수
func TimeCheck(obj map[string]interface{}) (model.UpdateFlags, error) {
	// 시간 설정
	var now time.Time
	var err error
	if obj["TEST_TIME"] == "0000-00-00 00:00:00" {
		now = time.Now().Round(time.Hour)
	} else {
		now, err = time.Parse("2006-01-02 15:04:05", obj["TEST_TIME"].(string))
		if err != nil {
			return model.UpdateFlags{}, fmt.Errorf("시간 파싱 실패: %w", err)
		}
		now = now.Round(time.Hour)
	}

	chkType := obj["TYPE"].(string)
	day := now.Day()
	hour := now.Hour()

	var coinUpdate, seasonUpdate, splitUpdate, insightUpdate bool

	if chkType == "dev" {
		coinUpdate = true
		seasonUpdate = hour%2 == 0
		splitUpdate = hour%2 == 1
	} else {
		if hour == 0 {
			coinUpdate = true
			seasonUpdate = day == 1 || day == 16
			splitUpdate = day == 8 || day == 23
		}
	}

	if hour == 0 {
		insightUpdate = true
	}

	return model.UpdateFlags{
		Season:  seasonUpdate,
		Split:   splitUpdate,
		Coin:    coinUpdate,
		Insight: insightUpdate,
	}, nil
}
