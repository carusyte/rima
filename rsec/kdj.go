package rsec

import (
	"github.com/carusyte/stock/model"
	rm "github.com/carusyte/rima/model"
	"fmt"
	"math"
	"sync"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/gio"
	"github.com/montanaflynn/stats"
	"github.com/pkg/errors"
	"reflect"
	"github.com/chrislusf/gleam/util"
	"time"
	"github.com/carusyte/rima/db"
	"github.com/carusyte/rima/cache"
	logr "github.com/sirupsen/logrus"
	"strings"
)

var (
	kdjFdMap          = make(map[string][]*model.KDJfdView)
	lock              = sync.RWMutex{}
	KdjScorer         = gio.RegisterMapper(kdjScoreMapper)
	KdjScoreCollector = gio.RegisterReducer(kdjScoreReducer)
)

type IndcScorer struct{}

// Deprecated. Use DataSync.SyncKdjFd instead.
func (s *IndcScorer) InitKdjFeatDat(fdMap *map[string][]*model.KDJfdView, reply *bool) error {
	logr.Infof("IndcScorer.InitKdjFeatDat called, fdmap size: %d", len(*fdMap))
	lock.Lock()
	defer lock.Unlock()
	kdjFdMap = *fdMap
	*reply = true
	logr.Infof("IndcScorer.InitKdjFeatDat finished. fdmap size: %d", len(kdjFdMap))
	return nil
}

//Score by assessing the historical data against the sampled feature data.
func (s *IndcScorer) ScoreKdj(req *rm.KdjScoreReq, rep *rm.KdjScoreRep) error {
	//call gleam api to map and reduce
	logr.Infof("IndcScorer.ScoreKdj called, input size: %d", len(req.Data))
	mapSource := getKdjMapSource(req)
	shard := 4.0
	shard, e := stats.Round(math.Pow(math.Log(float64(len(req.Data))), math.SqrtPi*math.Sqrt2), 0)
	if e != nil {
		return e
	}
	logr.Infof("#shard: %.0f", shard)
	sortOption := (&flow.SortOption{}).By(1, true)
	rep.Scores = make([]float64, 0, 16)
	rep.RowIds = make([]string, 0, 16)
	f := flow.New("KDJ Score Calculation").Slices(mapSource).RoundRobin("rr", int(shard)).
		Map("kdjScorer", KdjScorer).
		ReduceBy("kdjScoreCollector", KdjScoreCollector, sortOption).
		OutputRow(func(r *util.Row) error {
		logr.Debugf("Output Row: %+v", r)
		rep.RowIds = append(rep.RowIds, r.K[0].(string))
		rep.Scores = append(rep.Scores, r.V[0].(float64))
		return nil
	})

	if len(req.Data) >= 4 {
		option := distributed.Option().SetDataCenter("defaultDataCenter").
			SetMaster("localhost:45326")
		option.Rack = "defaultRack"
		f.Run(option)
	} else {
		f.Run()
	}
	logr.Infof("IndcScorer.ScoreKdj finished, score size: %d", len(rep.Scores))
	return nil
}

func getKdjMapSource(req *rm.KdjScoreReq) [][]interface{} {
	r := make([][]interface{}, len(req.Data))
	for i, ks := range req.Data {
		r[i] = make([]interface{}, 1)
		m := make(map[string]interface{})
		r[i][0] = m
		m["WgtDay"] = req.WgtDay
		m["WgtWeek"] = req.WgtWeek
		m["WgtMonth"] = req.WgtMonth
		m["KdjDay"], m["KdjWeek"], m["KdjMonth"] = cvtKdjSeries(ks)
		m["DayLen"] = len(ks.KdjDy)
		m["WeekLen"] = len(ks.KdjWk)
		m["MonthLen"] = len(ks.KdjMo)
		m["RowId"] = ks.RowId
		//rm["BuyDay"], rm["SellDay"] = getKDJfdMaps(model.DAY, len(ks.KdjDy))
		//rm["BuyWeek"], rm["SellWeek"] = getKDJfdMaps(model.WEEK, len(ks.KdjWk))
		//rm["BuyMonth"], rm["SellMonth"] = getKDJfdMaps(model.MONTH, len(ks.KdjMo))
	}
	return r
}

func cvtKdjSeries(ks *rm.KdjSeries) (day, week, month map[string][]float64) {
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

func getKDJfdMaps(cytp model.CYTP, len int) (buy, sell []map[string]interface{}, e error) {
	buy = make([]map[string]interface{}, 0, 1024)
	sell = make([]map[string]interface{}, 0, 1024)
	for i := -2; i < 3; i++ {
		n := len + i
		if n >= 2 {
			buyViews, e := kdjFdFrmDb(cytp, "BY", n)
			if e != nil {
				return nil, nil, e
			}
			sellViews, e := kdjFdFrmDb(cytp, "SL", n)
			if e != nil {
				return nil, nil, e
			}
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

func getKDJfdViews(cytp model.CYTP, num int) (buy, sell []*model.KDJfdView, e error) {
	buy = make([]*model.KDJfdView, 0, 1024)
	sell = make([]*model.KDJfdView, 0, 1024)
	for i := -2; i < 3; i++ {
		n := num + i
		if n >= 2 {
			nbuy, e := kdjFdFrmCb(cytp, "BY", n)
			if e != nil && !strings.Contains(e.Error(), "key not found") {
				return nil, nil, e
			}
			if nbuy != nil && len(nbuy) > 0 {
				buy = append(buy, nbuy...)
			}
			nsell, e := kdjFdFrmCb(cytp, "SL", n)
			if e != nil && !strings.Contains(e.Error(), "key not found") {
				return nil, nil, e
			}
			if nsell != nil && len(nsell) > 0 {
				sell = append(sell, nsell...)
			}
		}
	}
	logr.Debugf("getting kdj fd views, cytp: %s, num: %d,\n buys:%+v,\n sells:%+v", cytp, num, len(buy), len(sell))
	return
}

func kdjFdFrmCb(cytp model.CYTP, bysl string, num int) (fdvs []*model.KDJfdView, e error) {
	mk := kdjFdMapKey(cytp, bysl, num)
	_, e = cache.Cb().Get(mk, &fdvs)
	return;
}

func kdjFdFrmDb(cytp model.CYTP, bysl string, num int) ([]*model.KDJfdView, error) {
	mk := kdjFdMapKey(cytp, bysl, num)
	lock.Lock()
	defer lock.Unlock()

	if fdvs, exists := kdjFdMap[mk]; exists {
		return fdvs, nil
	}
	start := time.Now()
	rows, e := db.Ora().Query(db.SQL_KDJ_FEAT_DAT, cytp, bysl, num)
	if e != nil {
		if "sql: no rows in result set" == e.Error() {
			//FIXME what's the corresponding error message in Oracle?
			fdvs := make([]*model.KDJfdView, 0)
			kdjFdMap[mk] = fdvs
			return fdvs, nil
		} else {
			return nil, errors.Wrap(e, "failed to query kdj feat dat")
		}
	}
	defer rows.Close()
	var (
		fid                string
		pfid               string
		smpNum, fdNum, seq int
		weight, k, d, j    float64
		kfv                *model.KDJfdView
	)
	fdvs := make([]*model.KDJfdView, 0, 16)
	for rows.Next() {
		rows.Scan(&fid, &smpNum, &fdNum, &weight, &seq, &k, &d, &j)
		if fid != pfid {
			kfv = newKDJfdView(fid, bysl, cytp, smpNum, fdNum, weight)
			fdvs = append(fdvs, kfv)
		}
		kfv.Add(k, d, j)
		pfid = fid
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to query kdj feat dat.")
	}
	kdjFdMap[mk] = fdvs
	logr.Debugf("query kdj_feat_dat(%s,%s,%d): %.2f", cytp, bysl, num, time.Since(start).Seconds())
	return fdvs, nil
}

func newKDJfdView(fid, bysl string, cytp model.CYTP, smpNum, fdNum int, weight float64) *model.KDJfdView {
	v := &model.KDJfdView{}
	v.Indc = "KDJ"
	v.Cytp = model.CYTP(cytp)
	v.Fid = fid
	v.Bysl = bysl
	v.SmpNum = smpNum
	v.FdNum = fdNum
	v.Weight = weight
	v.K = make([]float64, 0, 16)
	v.D = make([]float64, 0, 16)
	v.J = make([]float64, 0, 16)
	return v
}

func kdjFdMapKey(cytp model.CYTP, bysl string, num int) string {
	return fmt.Sprintf("%s-%s-%d", cytp, bysl, num)
}

func kdjScoreMapper(row []interface{}) (e error) {
	defer func() {
		if r := recover(); r != nil {
			logr.Errorf("kdjScoreMapper.recover() is not nil: %+v", r)
			if er, ok := r.(error); ok {
				e = errors.Wrapf(er, "failed to execute kdjScoreMapper(), %+v", row)
			}
		}
	}()
	s := .0
	//interpRow(row)
	m := row[0].([]interface{})[0].(map[string]interface{})
	rowId := m["RowId"].(string)
	//in := row[0].([]interface{})[0].(*KdjScoreCalcInput)
	logr.Debugf("%s kdj score mapper receive row len: %d, row[0] len: %d,"+
		" parse map from input: %+v", rowId, len(row), len(row[0].([]interface{})), m)
	buyDay, sellDay, e := getKDJfdViews(model.DAY, int(gio.ToInt64(m["DayLen"])))
	logr.Debugf("%s day len: %+v, buys: %d, sells: %d, error: %+v", rowId, m["DayLen"],
		len(buyDay), len(sellDay), e)
	if e != nil {
		return e
	}
	sdy, e := calcKdjScore(m["KdjDay"].(map[string]interface{}), buyDay, sellDay)
	logr.Debugf("%s Day Score: %.2f, error: %+v", rowId, sdy, e)
	if e != nil {
		return e
	}
	buyWeek, sellWeek, e := getKDJfdViews(model.WEEK, int(gio.ToInt64(m["WeekLen"])))
	logr.Debugf("%s week len: %+v, buys: %d, sells: %d, error: %+v", rowId, m["WeekLen"],
		len(buyWeek), len(sellWeek), e)
	if e != nil {
		return e
	}
	swk, e := calcKdjScore(m["KdjWeek"].(map[string]interface{}), buyWeek, sellWeek)
	logr.Debugf("%s Week Score: %.2f, error: %+v", rowId, swk, e)
	if e != nil {
		return e
	}
	buyMonth, sellMonth, e := getKDJfdViews(model.MONTH, int(gio.ToInt64(m["MonthLen"])))
	logr.Debugf("%s month len: %+v, buys: %d, sells: %d, error: %+v", rowId, m["MonthLen"],
		len(buyMonth), len(sellMonth), e)
	if e != nil {
		return e
	}
	smo, e := calcKdjScore(m["KdjMonth"].(map[string]interface{}), buyMonth, sellMonth)
	logr.Debugf("%s Month Score: %.2f, error: %+v", rowId, smo, e)
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
	logr.Debugf("%s calculated score: %f, emitting", rowId, s)
	gio.Emit(rowId, s)
	//gio.Emit("KDJS", 2.13)

	return nil
}

//figure out the format of row
func interpRow(row []interface{}) {
	logr.Debugf("kdjScoreMapper param type: %+v, row len: %d", reflect.TypeOf(row), len(row))
	for i, ie := range row {
		logr.Infof("row[%d] type: %+v, value: %+v", i, reflect.TypeOf(ie), ie)
		switch ie.(type) {
		case []interface{}:
			a := ie.([]interface{})
			logr.Debugf("row[%d] is type []interface{}, size: %d, iterating the array:", i, len(a))
			for j, ia := range a {
				logr.Debugf("a[%d] is type %+v", j, reflect.TypeOf(ia))
				// more to be explored...
				switch ia.(type) {
				case map[interface{}]interface{}:
					m := ia.(map[interface{}]interface{})
					logr.Debugf("a[%d] map size: %d, iterating the map:", j, len(m))
					for k, v := range m {
						logr.Debugf("k: %+v (%+v)\tv: %+v (%+v)", k, reflect.TypeOf(k), v, reflect.TypeOf(v))
					}
				}
			}
		case map[interface{}]interface{}:
			m := ie.(map[interface{}]interface{})
			logr.Debugf("row[%d] map size: %d, iterating the map:", i, len(m))
			for k, v := range m {
				logr.Debugf("k: %+v\tv: %+v", k, v)
			}
		}
	}
}

func interpIntf(id string, intf interface{}) {
	logr.Debugf("%s intf type: %+v,  value: %+v", id, reflect.TypeOf(intf), intf)
	switch intf.(type) {
	case []interface{}:
		a := intf.([]interface{})
		logr.Debugf("%s intf is type []interface{}, size: %d, iterating the array:", id, len(a))
		for j, ia := range a {
			logr.Debugf("%s[%d] is type %+v: %+v", id, j, reflect.TypeOf(ia), ia)
			// more to be explored...
			switch ia.(type) {
			case map[interface{}]interface{}:
				m := ia.(map[interface{}]interface{})
				logr.Debugf("%s[%d] map size: %d, iterating the map:", id, j, len(m))
				for k, v := range m {
					logr.Debugf("%s,  k: %+v (%+v)\tv: %+v (%+v)", id, k, reflect.TypeOf(k), v, reflect.TypeOf(v))
				}
			}
		}
	case map[interface{}]interface{}:
		m := intf.(map[interface{}]interface{})
		logr.Debugf("%s intf map size: %d, iterating the map:", id, len(m))
		for k, v := range m {
			logr.Debugf("%s, k(%+v): %+v\tv(%+v): %+v", id, reflect.TypeOf(k), k, reflect.TypeOf(v), v)
		}
	}
}

func calcKdjScore(kdj map[string]interface{}, buyfds, sellfds []*model.KDJfdView) (s float64, e error) {
	logr.Debugf("kdj score calculation, input:%+v, buy len:%d, sell len:%d", kdj, len(buyfds), len(sellfds))
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
	logr.Debugf("kdj score calculation, bdi:%f, sdi:%f, score:%f", bdi, sdi, s)
	return s, nil
}

func kdjScoreReducer(x, y interface{}) (ret interface{}, e error) {
	defer func() {
		if r := recover(); r != nil {
			logr.Errorf("kdjScoreReducer.recover() is not nil: %+v", r)
			if er, ok := r.(error); ok {
				e = errors.Wrapf(er, "failed to execute kdjScoreReducer(), x:%+v, y:%+v", x, y)
			}
		}
	}()
	//interpIntf("x", x)
	//interpIntf("y", y)
	var r []interface{}
	switch x.(type) {
	case float64:
		r = make([]interface{}, 1)
		r[0] = x
	case []interface{}:
		r = x.([]interface{})
	case []float64:
		r = make([]interface{}, 0, 16)
		for _, ix := range x.([]float64) {
			r = append(r, ix)
		}
	}
	switch y.(type) {
	case float64:
		r = append(r, y)
	case []interface{}:
		r = append(r, y.([]interface{})...)
	case []float64:
		for _, iy := range y.([]float64) {
			r = append(r, iy)
		}
	}
	logr.Debugf("final result in reducer: %+v, %+v", reflect.TypeOf(r), r)
	return r, nil
}

// Evaluates KDJ DEVIA indicator against pruned feature data, returns the following result:
// Ratio of high DEVIA, ratio of positive DEVIA, mean of positive DEVIA, and DEVIA indicator, ranging from 0 to 1
func calcKdjDI(hist map[string]interface{}, fdvs []*model.KDJfdView) (hdr, pdr, mpd, di float64, e error) {
	defer func() {
		if r := recover(); r != nil {
			logr.Errorf("calcKdjDI.recover() is not nil: %+v", r)
			if er, ok := r.(error); ok {
				e = errors.Wrap(er, "failed to execute calcKdjDI")
			}
		}
	}()
	if len(hist) == 0 {
		return 0, 0, 0, 0, nil
	}
	pds := make([]float64, 0, 16)
	for _, fd := range fdvs {
		wgt := fd.Weight
		bkd, e := bestKdjDevi(hist["K"], hist["D"], hist["J"], fd.K, fd.D, fd.J)
		logr.Debugf("sk:%+v, sd:%+v, sj:%+v, tk:%+v, td:%+v, tj:%+v, best kdj devi: %f, error: %+v",
			hist["K"], hist["D"], hist["J"], fd.K, fd.D, fd.J, bkd, e)
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
	logr.Debugf("pds: %+v", pds)
	if len(pds) > 0 {
		mpd, e = stats.Mean(pds)
		if e != nil {
			e = errors.Wrap(e, "failed to calculate mean of positive devia")
			return 0, 0, 0, 0, e
		}
	}
	di = 0.5 * math.Min(1, math.Pow(hdr+0.92, 50))
	di += 0.3 * math.Min(1, math.Pow(math.Log(pdr+1), 0.37)+0.4*math.Pow(pdr, math.Pi)+math.Pow(pdr, 0.476145))
	di += 0.2 * math.Min(1, math.Pow(math.Log(math.Pow(mpd, math.E*math.Pi/1.1)+1), 0.06)+
		math.E/1.25/math.Pi*math.Pow(mpd, math.E*math.Pi))
	logr.Debugf("kdj di calculation, hdr: %f, pdr: %f, mpd: %f, di: %f", hdr, pdr, mpd, di)
	return
}

// Calculates the best match KDJ DEVIA, len(sk)==len(sd)==len(sj),
// and len(sk) and len(tk) can vary.
// DEVIA ranges from negative infinite to 1, with 1 indicating the most relevant KDJ data sets.
func bestKdjDevi(ski, sdi, sji interface{}, tk, td, tj []float64) (float64, error) {
	//should we also consider the len(x) to weigh the final result?
	sk := ski.([]interface{})
	sd := sdi.([]interface{})
	sj := sji.([]interface{})
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

func CalcKdjDevi(sk, sd, sj []interface{}, tk, td, tj []float64) (float64, error) {
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

func Devi(a []interface{}, b []float64) (float64, error) {
	if len(a) != len(b) || len(a) == 0 {
		return 0, errors.New("invalid input")
	}
	s := .0
	for i := 0; i < len(a); i++ {
		s += math.Pow(gio.ToFloat64(a[i])-gio.ToFloat64(b[i]), 2)
	}
	return math.Pow(s/float64(len(a)), 0.5), nil
}
