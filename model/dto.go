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

type KdjScoreRep struct {
	RowIds []string
	Scores []float64
	Detail []map[string]interface{}
}

type KdjPruneReq struct {
	ID   string
	Prec float64
	Pass int
	Data []*model.KDJfdView
}

type KdjPruneRep struct {
	Data []*model.KDJfdView
}
