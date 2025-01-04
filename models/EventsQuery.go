package models

type EventsInputData struct {
	Index      string `json:"index"`
	Collection string `json:"collection"`
	LastReadID int    `json:"lastreadid"`
}
