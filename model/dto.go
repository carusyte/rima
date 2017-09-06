package model

import "github.com/carusyte/stock/model"

type KdjScoreReq struct {
	Data                      []*KdjSeries
	WgtDay, WgtWeek, WgtMonth float64
}

type KdjSeries struct {
	RowId string
	// KDJ data of day, week and month
	KdjDy, KdjWk, KdjMo []*model.Indicator
}

type KdjScoreCalcInput struct {
	*KdjSeries
	WgtDay, WgtWeek, WgtMonth                   float64
	BuyDy, BuyWk, BuyMo, SellDy, SellWk, SellMo []*model.KDJfdView
}

type KdjScoreRep struct {
	RowIds []string
	Scores []float64
	Detail []map[string]interface{}
}

type KdjScore struct {
	Score                             float64
	BuyHdr, BuyPdr, BuyMpd, BuyDi     float64
	SellHdr, SellPdr, SellMpd, SellDi float64
}
