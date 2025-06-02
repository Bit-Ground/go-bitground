package model

// CoinSymbol 코인 심볼 구조체
type CoinSymbol struct {
	Id         int     `db:"id"`
	Symbol     string  `db:"symbol"`
	ChangeRate float64 `db:"change_rate"`
	IsCaution  bool    `db:"is_caution"`
	IsWarning  bool    `db:"is_warning"`
	IsDeleted  bool    `db:"is_deleted"`
	KoreanName string  `db:"korean_name"`
	TradePrice int64   `db:"trade_price_24h"`
}

// UpbitCoinList Upbit API에서 사용하는 코인 마켓 리스트 구조체
type UpbitCoinList struct {
	Market      string      `json:"market"`
	KoreanName  string      `json:"korean_name"`
	MarketEvent MarketEvent `json:"market_event"`
}

// MarketEvent Upbit API에서 사용하는 마켓 이벤트 구조체
type MarketEvent struct {
	Warning bool `json:"warning"`
	Caution struct {
		PriceFluctuations            bool `json:"PRICE_FLUCTUATIONS"`
		TradingVolumeSoaring         bool `json:"TRADING_VOLUME_SOARING"`
		DepositAmountSoaring         bool `json:"DEPOSIT_AMOUNT_SOARING"`
		GlobalPriceDifferences       bool `json:"GLOBAL_PRICE_DIFFERENCES"`
		ConcentrationOfSmallAccounts bool `json:"CONCENTRATION_OF_SMALL_ACCOUNTS"`
	} `json:"caution"`
}

// UpbitCoinPrice api 응답으로 받아온 현 코인 시세
type UpbitCoinPrice struct {
	Market           string  `json:"market"`
	TradePrice       float64 `json:"trade_price"`
	AccTradePrice    float64 `json:"acc_trade_price_24h"`
	PrevClosingPrice float64 `json:"prev_closing_price"`
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
