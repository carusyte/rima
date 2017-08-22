package rsec

import (
	"github.com/carusyte/stock/model"
	"fmt"
	"math"
	"sync"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/gio"
	"github.com/montanaflynn/stats"
	"github.com/pkg/errors"
	"log"
	"reflect"
	"github.com/chrislusf/gleam/util"
)

var (
	kdjFdMap          map[string][]*model.KDJfdView
	lock              = sync.RWMutex{}
	KdjScorer         = gio.RegisterMapper(kdjScoreMapper)
	KdjScoreCollector = gio.RegisterReducer(kdjScoreReducer)
)

type IndcScorer struct{}

type KdjScoreReq struct {
	Data                      []*KdjSeries
	WgtDay, WgtWeek, WgtMonth float64
}

type KdjSeries struct {
	// KDJ data of day, week and month
	KdjDy, KdjWk, KdjMo []*model.Indicator
}

type KdjScoreCalcInput struct {
	*KdjSeries
	WgtDay, WgtWeek, WgtMonth                   float64
	BuyDy, BuyWk, BuyMo, SellDy, SellWk, SellMo []*model.KDJfdView
}

type KdjScoreRep struct {
	Scores []float64
}

type KdjScore struct {
	Score                             float64
	BuyHdr, BuyPdr, BuyMpd, BuyDi     float64
	SellHdr, SellPdr, SellMpd, SellDi float64
}

func (s *IndcScorer) InitKdjFeatDat(fdMap *map[string][]*model.KDJfdView, reply *bool) error {
	log.Printf("IndcScorer.InitKdjFeatDat called, fdmap size: %d", len(*fdMap))
	lock.Lock()
	defer lock.Unlock()
	kdjFdMap = *fdMap
	*reply = true
	log.Printf("IndcScorer.InitKdjFeatDat finished. fdmap size: %d", len(kdjFdMap))
	return nil
}

//Score by assessing the historical data against the sampled feature data.
func (s *IndcScorer) ScoreKdj(req *KdjScoreReq, rep *KdjScoreRep) error {
	//call gleam api to map and reduce
	log.Printf("IndcScorer.ScoreKdj called, input size: %d", len(req.Data))
	mapSource := getKdjMapSource(req)
	shard := 4.0
	shard, e := stats.Round(math.Pow(math.Log(float64(len(req.Data))), math.SqrtPi), 0)
	if e != nil {
		return e
	}
	rep.Scores = make([]float64, len(req.Data))
	f := flow.New("KDJ Score Calculation").Slices(mapSource).Partition("partition", int(shard)).
		Map("kdjScorer", KdjScorer). // invoke the registered "kdjScorer" mapper function.
		ReduceBy("kdjScoreCollector", KdjScoreCollector). // invoke the registered "kdjScoreCollector" reducer function.
		OutputRow(func(r *util.Row) error {
		for i, v := range r.V[0].([]interface{}) {
			rep.Scores[i] = v.(float64)
		}
		return nil
	})

	if len(req.Data) >= 4 {
		f.Run(distributed.Option())
	} else {
		f.Run()
	}
	log.Printf("IndcScorer.ScoreKdj finished, score size: %d", len(rep.Scores))
	return nil
}

func getKdjMapSource(req *KdjScoreReq) [][]interface{} {
	r := make([][]interface{}, len(req.Data))
	for i, ks := range req.Data {
		r[i] = make([]interface{}, 1)
		// TODO no entity dto needed, use combination of slice and map instead
		m := make(map[string]interface{})
		r[i][0] = m
		m["WgtDay"] = req.WgtDay
		m["WgtWeek"] = req.WgtWeek
		m["WgtMonth"] = req.WgtMonth
		m["KdjDay"], m["KdjWeek"], m["KdjMonth"] = cvtKdjSeries(ks)
		m["BuyDay"], m["SellDay"] = getKDJfdMaps(model.DAY, len(ks.KdjDy))
		m["BuyWeek"], m["SellWeek"] = getKDJfdMaps(model.WEEK, len(ks.KdjWk))
		m["BuyMonth"], m["SellMonth"] = getKDJfdMaps(model.MONTH, len(ks.KdjMo))
	}
	return r
}

func cvtKdjSeries(ks *KdjSeries) (day, week, month map[string][]float64) {
	day = make(map[string][]float64)
	day["K"] = make([]float64, len(ks.KdjDy))
	day["D"] = make([]float64, len(ks.KdjDy))
	day["J"] = make([]float64, len(ks.KdjDy))
	for i, d := range ks.KdjDy {
		day["K"][i] = d.KDJ_K
		day["D"][i] = d.KDJ_D
		day["J"][i] = d.KDJ_J
	}

	week = make(map[string][]float64)
	week["K"] = make([]float64, len(ks.KdjWk))
	week["D"] = make([]float64, len(ks.KdjWk))
	week["J"] = make([]float64, len(ks.KdjWk))
	for i, d := range ks.KdjWk {
		week["K"][i] = d.KDJ_K
		week["D"][i] = d.KDJ_D
		week["J"][i] = d.KDJ_J
	}

	month = make(map[string][]float64)
	month["K"] = make([]float64, len(ks.KdjMo))
	month["D"] = make([]float64, len(ks.KdjMo))
	month["J"] = make([]float64, len(ks.KdjMo))
	for i, d := range ks.KdjMo {
		month["K"][i] = d.KDJ_K
		month["D"][i] = d.KDJ_D
		month["J"][i] = d.KDJ_J
	}
	return
}

func getKDJfdMaps(cytp model.CYTP, len int) (buy, sell []map[string]interface{}) {
	buy = make([]map[string]interface{}, 0, 1024)
	sell = make([]map[string]interface{}, 0, 1024)
	for i := -2; i < 3; i++ {
		n := len + i
		if n >= 2 {
			buyViews := GetKdjFeatDat(cytp, "BY", n)
			sellViews := GetKdjFeatDat(cytp, "SL", n)
			for _, v := range buyViews {
				m := make(map[string]interface{})
				m["Weight"] = v.Weight
				m["K"] = v.K
				m["D"] = v.D
				m["J"] = v.J
				buy = append(buy, m)
			}
			for _, v := range sellViews {
				m := make(map[string]interface{})
				m["Weight"] = v.Weight
				m["K"] = v.K
				m["D"] = v.D
				m["J"] = v.J
				sell = append(sell, m)
			}
		}
	}
	return
}

func GetKdjFeatDat(cytp model.CYTP, bysl string, num int) []*model.KDJfdView {
	mk := kdjFdMapKey(cytp, bysl, num)
	lock.RLock()
	defer lock.RUnlock()
	return kdjFdMap[mk];
}

func kdjFdMapKey(cytp model.CYTP, bysl string, num int) string {
	return fmt.Sprintf("%s-%s-%d", cytp, bysl, num)
}

func kdjScoreMapper(row []interface{}) error {
	s := .0
	//interpRow(row)
	m := row[0].([]interface{})[0].(map[interface{}]interface{})
	//in := row[0].([]interface{})[0].(*KdjScoreCalcInput)
	sdy, e := calcKdjScore(m["KdjDay"].(map[interface{}]interface{}), m["BuyDay"].([]interface{}),
		m["SellDay"].([]interface{}))
	if e != nil {
		return e
	}
	swk, e := calcKdjScore(m["KdjWeek"].(map[interface{}]interface{}), m["BuyWeek"].([]interface{}),
		m["SellWeek"].([]interface{}))
	if e != nil {
		return e
	}
	smo, e := calcKdjScore(m["KdjMonth"].(map[interface{}]interface{}), m["BuyMonth"].([]interface{}),
		m["SellMonth"].([]interface{}))
	if e != nil {
		return e
	}
	wgtDay := gio.ToFloat64(m["WgtDay"])
	wgtWeek := gio.ToFloat64(m["WgtWeek"])
	wgtMonth := gio.ToFloat64(m["WgtMonth"])
	s += sdy * wgtDay
	s += swk * wgtWeek
	s += smo * wgtMonth
	s /= wgtDay + wgtWeek + wgtMonth
	s = math.Min(100, math.Max(0, s))

	//gio.Emit([]float64{s})
	gio.Emit("KDJS", s)
	//gio.Emit("KDJS", 2.13)

	return nil
}

//figure out the format of row
func interpRow(row []interface{}) {
	log.Printf("kdjScoreMapper param type: %+v, row len: %d", reflect.TypeOf(row), len(row))
	for i, ie := range row {
		log.Printf("row[%d] type: %+v, value: %+v", i, reflect.TypeOf(ie), ie)
		switch ie.(type) {
		case []interface{}:
			a := ie.([]interface{})
			log.Printf("row[%d] is type []interface{}, size: %d, iterating the array:", i, len(a))
			for j, ia := range a {
				log.Printf("a[%d] is type %+v", j, reflect.TypeOf(ia))
				// more to be explored...
				switch ia.(type) {
				case map[interface{}]interface{}:
					m := ia.(map[interface{}]interface{})
					log.Printf("a[%d] map size: %d, iterating the map:", j, len(m))
					for k, v := range m {
						log.Printf("k: %+v (%+v)\tv: %+v (%+v)", k, reflect.TypeOf(k), v, reflect.TypeOf(v))
					}
				}
			}
		case map[interface{}]interface{}:
			m := ie.(map[interface{}]interface{})
			log.Printf("row[%d] map size: %d, iterating the map:", i, len(m))
			for k, v := range m {
				log.Printf("k: %+v\tv: %+v", k, v)
			}
		}
	}
}

func interpIntf(id string, intf interface{}) {
	log.Printf("%s intf type: %+v,  value: %+v", id, reflect.TypeOf(intf), intf)
	switch intf.(type) {
	case []interface{}:
		a := intf.([]interface{})
		log.Printf("%s intf is type []interface{}, size: %d, iterating the array:", id, len(a))
		for j, ia := range a {
			log.Printf("%s[%d] is type %+v", id, j, reflect.TypeOf(ia))
			// more to be explored...
			switch ia.(type) {
			case map[interface{}]interface{}:
				m := ia.(map[interface{}]interface{})
				log.Printf("%s[%d] map size: %d, iterating the map:", id, j, len(m))
				for k, v := range m {
					log.Printf("%s,  k: %+v (%+v)\tv: %+v (%+v)", id, k, reflect.TypeOf(k), v, reflect.TypeOf(v))
				}
			}
		}
	case map[interface{}]interface{}:
		m := intf.(map[interface{}]interface{})
		log.Printf("%s intf map size: %d, iterating the map:", id, len(m))
		for k, v := range m {
			log.Printf("%s, k: %+v\tv: %+v", id, k, v)
		}
	}
}

func calcKdjScore(kdj map[interface{}]interface{}, buyfds, sellfds []interface{}) (s float64, e error) {
	_, _, _, bdi, e := calcKdjDI(kdj, buyfds)
	//val = fmt.Sprintf("%.2f/%.2f/%.2f/%.2f\n", hdr, pdr, mpd, bdi)
	if e != nil {
		return 0, e
	}
	_, _, _, sdi, e := calcKdjDI(kdj, sellfds)
	//val += fmt.Sprintf("%.2f/%.2f/%.2f/%.2f\n", hdr, pdr, mpd, sdi)
	if e != nil {
		return 0, e
	}
	dirat := .0
	s = .0
	if sdi == 0 {
		dirat = bdi
	} else {
		dirat = (bdi - sdi) / math.Abs(sdi)
	}
	if dirat > 0 && dirat < 0.995 {
		s = 30 * (0.0015 + 3.3609*dirat - 4.3302*math.Pow(dirat, 2.) + 2.5115*math.Pow(dirat, 3.) -
			0.5449*math.Pow(dirat, 4.))
	} else if dirat >= 0.995 {
		s = 30
	}
	if bdi > 0.201 && bdi < 0.81 {
		s += 70 * (0.0283 - 1.8257*bdi + 10.4231*math.Pow(bdi, 2.) - 10.8682*math.Pow(bdi, 3.) + 3.2234*math.Pow(bdi, 4.))
	} else if bdi >= 0.81 {
		s += 70
	}
	return s, nil
}

func kdjScoreReducer(x, y interface{}) (interface{}, error) {
	//interpIntf("x", x)
	//interpIntf("y", y)
	var r []interface{}
	switch x.(type) {
	case float64:
		r = make([]interface{}, 1)
		r[0] = x
	case []interface{}:
		r = x.([]interface{})
	}
	r = append(r, y)
	return r, nil
}

// Evaluates KDJ DEVIA indicator against pruned feature data, returns the following result:
// Ratio of high DEVIA, ratio of positive DEVIA, mean of positive DEVIA, and DEVIA indicator, ranging from 0 to 1
func calcKdjDI(hist map[interface{}]interface{}, fdvs []interface{}) (hdr, pdr, mpd, di float64, e error) {
	if len(hist) == 0 {
		return 0, 0, 0, 0, nil
	}
	pds := make([]float64, 0, 16)
	for _, fdi := range fdvs {
		fd := fdi.(map[interface{}]interface{})
		wgt := gio.ToFloat64(fd["Weight"])
		bkd, e := bestKdjDevi(hist["K"], hist["D"], hist["J"], fd["K"], fd["D"], fd["J"])
		if e != nil {
			return 0, 0, 0, 0, e
		}
		if bkd >= 0 {
			pds = append(pds, bkd)
			pdr += wgt
			if bkd >= 0.8 {
				hdr += wgt
			}
		}
	}
	if len(pds) > 0 {
		mpd, e = stats.Mean(pds)
		e = errors.Wrap(e, "failed to calculate mean of positive devia")
		return 0, 0, 0, 0, e
	}
	di = 0.5 * math.Min(1, math.Pow(hdr+0.92, 50))
	di += 0.3 * math.Min(1, math.Pow(math.Log(pdr+1), 0.37)+0.4*math.Pow(pdr, math.Pi)+math.Pow(pdr, 0.476145))
	di += 0.2 * math.Min(1, math.Pow(math.Log(math.Pow(mpd, math.E*math.Pi/1.1)+1), 0.06)+
		math.E/1.25/math.Pi*math.Pow(mpd, math.E*math.Pi))
	return
}

// Calculates the best match KDJ DEVIA, len(sk)==len(sd)==len(sj),
// and len(sk) and len(tk) can vary.
// DEVIA ranges from negative infinite to 1, with 1 indicating the most relevant KDJ data sets.
func bestKdjDevi(ski, sdi, sji, tki, tdi, tji interface{}) (float64, error) {
	//should we also consider the len(x) to weigh the final result?
	sk := ski.([]interface{})
	sd := sdi.([]interface{})
	sj := sji.([]interface{})
	tk := tki.([]interface{})
	td := tdi.([]interface{})
	tj := tji.([]interface{})
	dif := len(sk) - len(tk)
	if dif > 0 {
		cc := -100.0
		for i := 0; i <= dif; i++ {
			e := len(sk) - dif + i
			tcc, err := CalcKdjDevi(sk[i:e], sd[i:e], sj[i:e], tk, td, tj)
			if err != nil {
				return 0, err
			}
			if tcc > cc {
				cc = tcc
			}
		}
		return cc, nil
	} else if dif < 0 {
		cc := -100.0
		dif *= -1
		for i := 0; i <= dif; i++ {
			e := len(tk) - dif + i
			tcc, err := CalcKdjDevi(sk, sd, sj, tk[i:e], td[i:e], tj[i:e])
			if err != nil {
				return 0, err
			}
			if tcc > cc {
				cc = tcc
			}
		}
		return cc, nil
	} else {
		return CalcKdjDevi(sk, sd, sj, tk, td, tj)
	}
}

func CalcKdjDevi(sk, sd, sj, tk, td, tj []interface{}) (float64, error) {
	kcc, e := Devi(sk, tk)
	if e != nil {
		return 0, errors.New(fmt.Sprintf("failed to calculate kcc: %+v, %+v", sk, tk))
	}
	dcc, e := Devi(sd, td)
	if e != nil {
		return 0, errors.New(fmt.Sprintf("failed to calculate dcc: %+v, %+v", sd, td))
	}
	jcc, e := Devi(sj, tj)
	if e != nil {
		return 0, errors.New(fmt.Sprintf("failed to calculate jcc: %+v, %+v", sj, tj))
	}
	scc := (kcc*1.0 + dcc*4.0 + jcc*5.0) / 10.0
	return -0.001*math.Pow(scc, math.E) + 1, nil
}

func Devi(a, b []interface{}) (float64, error) {
	if len(a) != len(b) || len(a) == 0 {
		return 0, errors.New("invalid input")
	}
	s := .0
	for i := 0; i < len(a); i++ {
		s += math.Pow(gio.ToFloat64(a[i])-gio.ToFloat64(b[i]), 2)
	}
	return math.Pow(s/float64(len(a)), 0.5), nil
}
