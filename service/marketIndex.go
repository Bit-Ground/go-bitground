package service

import "database/sql"

type MarketIndexService struct {
	db *sql.DB
}

func NewMarketIndexService(db *sql.DB) *MarketIndexService {
	return &MarketIndexService{db: db}
}
