package service

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// RequestBody 는 Gemini API 요청 본문의 구조를 정의합니다.
type RequestBody struct {
	Contents []Content `json:"contents"`
}

// Content 는 사용자의 입력 내용을 정의합니다.
type Content struct {
	Parts []Part `json:"parts"`
}

// Part 는 텍스트 또는 다른 미디어 유형을 정의합니다.
type Part struct {
	Text string `json:"text"`
}

// ResponseBody 는 Gemini API 응답 본문의 구조를 정의합니다.
type ResponseBody struct {
	Candidates []Candidate `json:"candidates"`
}

// Candidate 는 모델이 생성한 하나의 가능한 응답을 정의합니다.
type Candidate struct {
	Content Content `json:"content"`
}

type Insight struct {
	Symbol  string `json:"symbol"`
	Insight string `json:"insight"`
	Score   int    `json:"score"`
}

func UpdateInsight(ctx context.Context, db *sql.DB, geminiKey string, symbolMap map[string]int) error {
	// 1. Gemini API를 사용하여 인사이트 데이터를 가져옵니다.
	insights, err := getInsightData(ctx, geminiKey, symbolMap)
	if err != nil {
		return fmt.Errorf("getInsightData 에러: %w", err)
	}

	if len(insights) == 0 {
		return fmt.Errorf("인사이트 데이터가 비어 있습니다. Gemini API 응답을 확인하세요")
	}

	// 2. 인사이트 데이터를 데이터베이스에 삽입합니다.
	err = insertInsights(ctx, db, insights)
	if err != nil {
		return fmt.Errorf("insertInsights 에러: %w", err)
	}

	return nil
}

// gemini api를 사용하여 인사이트 데이터를 가져오는 함수
func getInsightData(ctx context.Context, geminiKey string, symbolMap map[string]int) ([]Insight, error) {
	const apiURL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent"

	// 프롬프트 생성
	prompt := createPrompt()

	// 요청 본문 생성
	requestBody := RequestBody{
		Contents: []Content{
			{
				Parts: []Part{
					{Text: prompt},
				},
			},
		},
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return []Insight{}, fmt.Errorf("JSON 인코딩 오류: %w", err)
	}

	// Context를 활용한 HTTP 요청 (타임아웃: 3분)
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	// HTTP 요청 생성
	req, err := http.NewRequestWithContext(reqCtx, "POST", apiURL+"?key="+geminiKey, bytes.NewBuffer(jsonBody))
	if err != nil {
		return []Insight{}, fmt.Errorf("HTTP 요청 생성 에러: %w", err)
	}

	// http 클라이언트 생성
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return []Insight{}, fmt.Errorf("API 요청 에러: %w", err)
	}
	defer func(Body io.ReadCloser) {
		// 응답 본문 닫기
		if err := Body.Close(); err != nil {
			log.Printf("응답 본문 닫기 에러: %v\n", err)
		}
	}(resp.Body)

	// 응답 본문 읽기
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return []Insight{}, fmt.Errorf("응답 본문 읽기 에러: %w", err)
	}

	// HTTP 응답 상태 코드 확인
	if resp.StatusCode != http.StatusOK {
		return []Insight{}, fmt.Errorf("API 요청 실패: 상태 코드 %d, 응답: %s", resp.StatusCode, string(bodyBytes))
	}

	// 응답 본문 파싱
	var responseBody ResponseBody
	err = json.Unmarshal(bodyBytes, &responseBody)
	if err != nil {
		return []Insight{}, fmt.Errorf("JSON 디코딩 에러: %w", err)
	}

	// 응답에서 모델 출력 추출
	var insights []Insight
	if len(responseBody.Candidates) > 0 && len(responseBody.Candidates[0].Content.Parts) > 0 {
		generatedText := responseBody.Candidates[0].Content.Parts[0].Text // 첫 번째 Part의 텍스트 가져오기

		cleanedText := generatedText

		re := regexp.MustCompile("(?s)```json\\s*(.*?)\\s*```") // `(?s)`는 `.`이 줄바꿈 문자도 포함하도록 함

		matches := re.FindStringSubmatch(generatedText)
		if len(matches) > 1 {
			cleanedText = strings.TrimSpace(matches[1]) // ```json ... ``` 안의 내용만 추출
		} else {
			// 만약 ```json ... ``` 패턴이 없으면, 불필요한 바깥 괄호만 제거 시도
			cleanedText = strings.TrimPrefix(cleanedText, "[{[{")
			cleanedText = strings.TrimSuffix(cleanedText, "}]}]")
			cleanedText = strings.TrimSpace(cleanedText)
		}

		// JSON 파싱 시도
		err := json.Unmarshal([]byte(cleanedText), &insights)
		if err != nil {
			// 파싱 실패 시 원본 텍스트와 에러 메시지를 함께 반환하여 디버깅 용이
			return nil, fmt.Errorf("모델 응답 JSON 파싱 오류: %v\n원본 텍스트(정제 후): `%s`", err, cleanedText)
		}
	} else {
		return []Insight{}, fmt.Errorf("모델 응답에 유효한 데이터가 없습니다. 응답: %s", string(bodyBytes))
	}

	// 유효하지 않은 인사이트 필터링
	newInsights := filterInvalidInsights(insights, symbolMap)

	return newInsights, nil
}

// 프롬프트 생성
func createPrompt() string {
	now := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	prompt := fmt.Sprintf(`
		[지시사항: 당신은 전문적인 암호화폐 시장 분석가입니다. 사용자에게 포괄적이고 통찰력 있는 일일 시장 동향 분석을 제공해야 합니다. 모든 분석은 %s을 기준으로 합니다.]
		
		[역할 및 데이터 전제]:
		- 당신은 %s일 자의 최신 암호화폐 시장 데이터(가격, 거래량, 시가총액 등), 주요 뉴스, 거시 경제 지표 및 차트 정보를 종합적으로 분석할 수 있다고 가정합니다.
		- 당신의 분석은 투자 조언이 아니며, 정보 제공 목적임을 명심해야 합니다.
		- 매우 중요: 모든 분석 내용, 특히 뉴스, 파트너십, 기술 업데이트 등은 반드시 확인 가능하고 널리 알려진 사실에 근거해야 합니다. 불확실하거나 검증되지 않은 정보, 개인적인 추측, 루머는 절대로 생성해서는 안 됩니다. 만약 특정 정보에 대한 확신이 없다면, 해당 내용을 포함하지 않거나 '확인된 바 없음' 등으로 명시적으로 표현해야 합니다.
		
		[점수 평가 기준 (Rubric): 모든 점수는 아래 기준에 따라 1점에서 100점 사이로 부여합니다.]
		- 90-100점 (매우 긍정적/강한 강세): 다수의 명확하고 강력한 긍정적 촉매제가 존재하며, 시장 전반 또는 해당 코인에 대한 압도적인 낙관론이 우세한 상황. 단기적으로 심각한 리스크 요인이 거의 없거나 매우 제한적임. 기술적 지표들이 매우 강력한 상승 신호를 보임.
		- 70-89점 (긍정적/강세): 긍정적 요인이 부정적 요인보다 명확히 우세하며, 전반적으로 상승 기대감이 형성되는 상황. 일부 관리 가능한 리스크 요인이 존재할 수 있으나, 긍정적 전망이 지배적임. 기술적 지표들이 상승 추세를 지지함.
		- 60-69점 (중립적 강세 또는 긍정적 혼조세): 긍정적 요인과 부정적 요인이 혼재하나, 전반적으로 긍정적인 측면이 미세하게 우세하거나, 중요한 지지선에서 반등 시도 또는 박스권 상단 돌파 시도 등 기술적으로 약간의 긍정적 신호가 관찰되는 상황. 시장 참여자들의 의견이 엇갈리나, 상승에 대한 기대감이 조금 더 높은 편. 뚜렷한 상승 모멘텀은 부족하나 하방 경직성이 나타날 수 있음.
		- 50-59점 (중립적 약세 또는 부정적 혼조세): 긍정적 요인과 부정적 요인이 혼재하나, 전반적으로 부정적인 측면이 미세하게 우세하거나, 중요한 저항선 돌파에 실패 또는 박스권 하단 이탈 우려 등 기술적으로 약간의 부정적 신호가 관찰되는 상황. 큰 폭의 하락은 아니나 상승 동력이 뚜렷하게 부족하고 관망세가 짙어지거나 소폭의 조정 가능성이 있음.
		- 30-49점 (부정적/약세): 부정적 요인이 긍정적 요인보다 명확히 우세하며, 전반적으로 하락 우려가 형성되는 상황. 주요 지지선 이탈 위험 또는 이미 하락 추세가 진행 중일 수 있음. 회복을 위한 명확한 촉매제가 부족함.
		- 1-29점 (매우 부정적/강한 약세): 다수의 명확하고 강력한 부정적 촉매제가 존재하며, 시장 전반 또는 해당 코인에 대한 압도적인 비관론이 우세한 상황. 심각한 리스크 요인이 산재해 있으며 추가 하락 가능성이 매우 높음. 기술적 지표들이 매우 강력한 하락 신호를 보임.
		
		[점수 산정 시 내부 고려 사항]: 점수를 산정할 때는 다음 사항들을 내부적으로 심층 고려하여 [점수 평가 기준 (Rubric)]에 가장 부합하는 점수를 신중하게 결정하십시오:
		1. 핵심 질문 기반 질적 평가: 분석 대상에 대해 '단기적으로 상승 확률과 하락 확률 중 어느 쪽이 근소하게라도 우세한가?', '만약 중립적 상황이라면, 긍정적 측면과 부정적 측면 중 어느 쪽으로 미세하게 기울어져 있는가?', '주요 긍정/부정 요인의 실질적인 파급력과 지속성은 어느 정도인가?' 와 같은 핵심 질문에 대한 답을 내부적으로 명확히 하십시오.
		2. 요인 분석 기반 양적 평가 (내부적): 식별된 주요 긍정적 요인과 부정적 요인의 개수를 헤아리고, 각 요인의 예상되는 영향력의 강도(예: 매우 강함, 강함, 중간, 약함, 매우 약함) 및 시장에 영향을 미치는 시간적 범위(단기, 중기)를 내부적으로 평가하십시오.
		3. 종합 판단: 위의 질적 및 양적 평가 결과를 종합적으로 고려하여, [점수 평가 기준 (Rubric)]의 각 구간 설명 중 현재 분석된 상황을 가장 정확하게 반영하는 구간을 선택하고, 해당 구간 내에서 가장 적절하다고 판단되는 특정 점수를 부여하십시오.
		
		[첫 번째 요구사항: 전체 암호화폐 시장에 대한 종합적인 분석을 제공하십시오.]
		- 현재 시장 감성(강세, 약세, 중립/혼조세) 및 그 감성을 뒷받침하는 주요 요인들(긍정적 요인과 부정적 요인을 구분하여 명시).
		- 시장을 움직이는 핵심 동인 (예: 비트코인 현물 ETF 유입/유출, 미국 연준의 통화 정책, 글로벌 경제 상황, 주요 규제 발표, 기관 투자 동향 등).
		- 비트코인의 시장 지배력(도미넌스) 변화와 이것이 전체 알트코인 시장에 미치는 영향.
		- 전체 알트코인 시장의 요약된 동향 (예: 특정 섹터의 강세, 전반적인 BTC 추종 여부).
		- 단기 (향후 24-48시간) 전망 및 이 기간 동안 주의해야 할 주요 리스크 요인 (예: 중요한 경제 지표 발표, 주요 회의, 기술적 저항/지지선). 분석은 최대한 객관적이고 데이터 중심적인 어조를 유지하며, 과장된 표현이나 근거 없는 낙관/비관은 피해야 합니다. 모든 전망은 '가능성', '예상', '전망' 등의 신중한 용어를 사용하여 표현하십시오.
		- 이 분석에 대해 종합 점수를 부여하십시오.
		- 이 분석 결과의 'symbol'은 "MARKET_OVERALL"로 설정하십시오.
		
		[두 번째 요구사항: 다음 개별 코인들에 대해 간결하고 전문적인 분석을 제공하십시오.]
		- 각 코인의 현재 시장 동향, 최근 뉴스(긍정적/부정적 구분 및 해당 뉴스가 코인에 미칠 단기적 영향 예상 포함. - 만약 특정 코인에 대한 중요하거나 검증된 최신 뉴스가 없다면, 현재 가격 움직임이나 기술적 분석에 더 집중하십시오), 그리고 단기 전망에 초점을 맞추십시오.
		- 분석은 최대한 객관적이고 데이터 중심적인 어조를 유지하며, 과장된 표현이나 근거 없는 낙관/비관은 피해야 합니다. 모든 전망은 '가능성', '예상', '전망' 등의 신중한 용어를 사용하여 표현하십시오.
		- 각 코인 분석에 대해 개별 감성 점수를 부여하십시오.
		- 각 분석의 'symbol'은 해당 코인의 심볼(예: KRW-ETH)로 설정하십시오.
		
		[요청 코인 목록]:
		- 이더리움 (KRW-ETH)
		- 솔라나 (KRW-SOL)
		- 리플 (KRW-XRP)
		
		[추가 요구사항: 위 요청 목록 외에, 업비트에 상장되어 있는 코인 중 %s일 시장 트렌드 기준으로 거래자들이 특별히 관심 가질 만하거나, 관심 가져야 할 필요가 있는 코인(5개)이 있다면, 그 코인들에 대한 동향 분석을 중요도 순으로 위와 동일한 형식으로 추가하십시오.]
		- 선정 기준 및 제약 조건 (매우 중요):
		1. 업비트 상장 여부 (추정적 확인): 위 1번 조건을 만족하는 코인 중, %s일 기준으로 업비트(KRW 마켓)에 상장되어 있을 가능성이 매우 높다고 합리적으로 추정되는 코인을 우선적으로 고려합니다.
		2. 시장 관심도: 최근 거래량 급증, 주요 기술적 진전(업데이트), 중요한 파트너십 발표, 특정 투자 섹터에서의 급부상, 또는 단기적으로 높은 변동성을 보이는 등 객관적인 데이터나 이벤트에 기반하여 현재 시장 참여자들이 특별히 관심을 가질 만한 이유가 있는 코인을 선정합니다.
		3. 할루시네이션 절대 금지: 만약 위 조건들을 모두 만족하는 코인을 명확히 식별할 수 없다면, 절대로 존재하지 않는 코인 심볼이나 이름을 지어내서는 안 됩니다. 이 경우, 추가 코인 없이 결과를 반환하는 것이 훨씬 바람직합니다.
		- 추가되는 코인 또한 반드시 아래 JSON 객체 배열 형식의 규칙을 따라야 합니다. (점수 산정 시 위 [두 번째 요구사항]의 '[점수 산정 시 내부 고려 사항]'을 동일하게 따릅니다.)
		
		[출력 형식: 모든 분석 결과는 아래와 같은 JSON 객체의 배열 형태로만 제공되어야 합니다. JSON 구조와 필드명, 데이터 형식을 정확히 준수해야 하며, 요청된 분석 내용 외에 어떠한 부가 설명, 의견, 혹은 다른 어떤 텍스트도 포함해서는 안 됩니다. insight에 들어갈 내용은 한국어로, 지정된 요구사항을 바탕으로 핵심 내용을 요약하여 최대 500자 이내로 작성해야 합니다.]
		[
			{
				"symbol": "MARKET_OVERALL",
				"insight": "[첫 번째 요구사항]에 대한 답변(시장 감성, 핵심 동인, 비트코인 도미넌스, 알트코인 동향, 단기 전망/리스크)을 핵심 위주로 한국어 500자 이내로 작성합니다.",
				"score": 75 // 예시 점수, 실제 분석에 따른 점수로 대체
			},
			{
				"symbol": "KRW-ETH",
				"insight": "[두 번째 요구사항]에 따라 해당 코인의 현재 시장 동향, 주요 뉴스(영향 예상 및 부재 시 명시 포함), 단기 전망을 중심으로 한국어 500자 이내로 작성합니다.",
				"score": 82 // 예시 점수, 실제 분석에 따른 점수로 대체
			},
			{
				"symbol": "KRW-SOL",
				"insight": "[두 번째 요구사항]에 따라 해당 코인의 현재 시장 동향, 주요 뉴스(영향 예상 및 부재 시 명시 포함), 단기 전망을 중심으로 한국어 500자 이내로 작성합니다.",
				"score": 0 // 실제 분석에 따른 점수로 대체
			},
			{
				"symbol": "KRW-XRP",
				"insight": "[두 번째 요구사항]에 따라 해당 코인의 현재 시장 동향, 주요 뉴스(영향 예상 및 부재 시 명시 포함), 단기 전망을 중심으로 한국어 500자 이내로 작성합니다.",
				"score": 0 // 실제 분석에 따른 점수로 대체
			},
			// 추가 코인이 있다면 아래와 같은 형식으로 추가 (5개, 중요도 순)
			{
				"symbol": "KRW-ADDED_COIN_EXAMPLE", // 실제 코인 심볼로 대체
				"insight": "[두 번째 요구사항]의 분석 형식 및 [추가 요구사항]의 선정 이유를 일부 포함하여 해당 코인의 분석을 한국어 500자 이내로 작성합니다. 예: '최근 거래량 급증 및 주요 업데이트 발표로 시장 관심이 높은 KRW-ABC 코인은 단기적으로 변동성 확대 가능성이 있으며...'",
				"score": 0 // 해당 코인의 실제 점수로 대체
			}
		]
	`, now, now, now, now)

	return prompt
}

// 유효하지 않은 인사이트 데이터를 필터링
func filterInvalidInsights(insights []Insight, symbolMap map[string]int) []Insight {
	var newInsights []Insight
	for i, insight := range insights {
		if i < 4 { // 첫 번째 4개 인사이트는 전체 시장 분석이므로 무조건 추가
			newInsights = append(newInsights, insight)
			continue
		}
		// 심볼이 symbolMap에 존재하는지 확인
		if symbolMap[insight.Symbol] != 0 {
			newInsights = append(newInsights, insight)
		}
	}
	return newInsights
}

// 삽입 쿼리 수행
func insertInsights(ctx context.Context, db *sql.DB, insights []Insight) error {
	now := time.Now()

	// 트랜잭션 시작
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("트랜잭션 시작 에러: %w", err)
	}
	defer func() {
		if p := recover(); p != nil { // 패닉 발생 시 롤백
			_ = tx.Rollback()
			panic(p) // 패닉 다시 던지기
		}
		if err != nil { // 함수 종료 시 err가 nil이 아니면 롤백
			_ = tx.Rollback()
		}
	}()

	// 쿼리 타임아웃 설정 (20초)
	queryCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	query := `
		INSERT INTO ai_insights (symbol, insight, score, date)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			insight = VALUES(insight),
			score = VALUES(score)
	`
	stmt, err := tx.PrepareContext(queryCtx, query)
	if err != nil {
		_ = tx.Rollback() // 쿼리 준비 실패 시 롤백
		return fmt.Errorf("쿼리 준비 에러: %w", err)
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			log.Printf("쿼리 종료 에러: %v\n", err)
		}
	}()

	for _, insight := range insights {

		_, err := stmt.ExecContext(queryCtx, insight.Symbol, insight.Insight, insight.Score, now)
		if err != nil {
			_ = tx.Rollback() // 쿼리 실행 실패 시 롤백
			return fmt.Errorf("데이터베이스 삽입 에러: %w", err)
		}
	}

	// 모든 작업이 성공적으로 완료되었으므로 커밋 시도
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback() // 트랜잭션 커밋 실패 시 롤백
		return fmt.Errorf("트랜잭션 커밋 에러: %w", err)
	}

	return nil
}
