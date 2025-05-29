package service

import "database/sql"

type SeasonUpdateService struct {
	db *sql.DB
}

func NewSeasonUpdateService(db *sql.DB) *SeasonUpdateService {
	return &SeasonUpdateService{db: db}
}
