package model

// CoinSymbol 코인 심볼 구조체
type CoinSymbol struct {
	Id     int    `db:"id"`
	Symbol string `db:"symbol"`
}

// UpbitCoinPrice api 응답으로 받아온 현 코인 시세
type UpbitCoinPrice struct {
	Market     string  `json:"market"`
	TradePrice float64 `json:"trade_price"`
}

// UserCash 유저 현금 구조체
type UserCash struct {
	UserID int `db:"id"`
	Cash   int `db:"cash"`
}

// UserAsset 유저 자산 구조체
type UserAsset struct {
	UserID   int     `db:"user_id"`
	SymbolID int     `db:"symbol_id"`
	Amount   float64 `db:"amount"`
}

// UserTotalAsset 유저 총 자산 구조체 (랭킹 및 티어 포함)
type UserTotalAsset struct {
	UserID     int
	TotalValue int
	Rank       int
	Tier       int
}

// UpdateFlags 업데이트 필요 상태 나타내는 구조체
type UpdateFlags struct {
	Season  bool
	Split   bool
	Coin    bool
	Insight bool
}
