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
	rep = new(KdjScoreRep)
	f := flow.New("kdj score calculation").Slices(mapSource).Partition("partition", int(shard)).
		// TODO try to use Lua script instead, load Lua using dotsql at the moment
		Map("kdjScorer", KdjScorer). // invoke the registered "kdjScorer" mapper function.
		ReduceBy("kdjScoreCollector", KdjScoreCollector). // invoke the registered "kdjScoreCollector" reducer function.
		SaveFirstRowTo(&rep.Scores)

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
		in := new(KdjScoreCalcInput)
		// TODO no entity dto needed, use combination of slice and map instead
		r[i][0] = in
		in.KdjSeries = ks
		in.BuyDy, in.SellDy = getKDJfdViews(model.DAY, len(in.KdjDy))
		in.BuyWk, in.SellWk = getKDJfdViews(model.WEEK, len(in.KdjWk))
		in.BuyMo, in.SellMo = getKDJfdViews(model.MONTH, len(in.KdjMo))
		in.WgtDay = req.WgtDay
		in.WgtWeek = req.WgtWeek
		in.WgtMonth = req.WgtMonth
	}
	return r
}

func getKDJfdViews(cytp model.CYTP, len int) (buy, sell []*model.KDJfdView) {
	buy = make([]*model.KDJfdView, 0, 1024)
	sell = make([]*model.KDJfdView, 0, 1024)
	for i := -2; i < 3; i++ {
		n := len + i
		if n >= 2 {
			buy = append(buy, GetKdjFeatDat(cytp, "BY", n)...)
			sell = append(sell, GetKdjFeatDat(cytp, "SL", n)...)
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
	//FIXME figure out the format of row
	log.Printf("kdjScoreMapper param type: %+v, row len: %d", reflect.TypeOf(row), len(row))
	for i, ie := range row {
		log.Printf("row[%d] type: %+v", i, reflect.TypeOf(ie))
		switch row[i].(type) {
		case []interface{}:
			a := row[i].([]interface{})
			log.Printf("row[%d] is type []interface{}, size: %d, iterating the array:", i, len(a))
			for j, ia := range a {
				log.Printf("a[%d] is type %+v", j, reflect.TypeOf(ia))
				// more to be explored...
				switch ia.(type) {
				case map[interface{}]interface{}:
					m := ia.(map[interface{}]interface{})
					log.Printf("a[%d] map size: %d, iterating the map:", j, len(m))
					for k, v := range m {
						log.Printf("k: %+v\tv: %+v", k, v)
					}
				}
			}
		case map[interface{}]interface{}:
			m := row[i].(map[interface{}]interface{})
			log.Printf("row[%d] map size: %d, iterating the map:", i, len(m))
			for k, v := range m {
				log.Printf("k: %+v\tv: %+v", k, v)
			}
		}
	}
	in := row[0].([]interface{})[0].(*KdjScoreCalcInput)
	sdy, e := calcKdjScore(in.KdjDy, in.BuyDy, in.SellDy)
	if e != nil {
		return e
	}
	swk, e := calcKdjScore(in.KdjWk, in.BuyWk, in.SellWk)
	if e != nil {
		return e
	}
	smo, e := calcKdjScore(in.KdjMo, in.BuyMo, in.SellMo)
	if e != nil {
		return e
	}
	s += sdy * in.WgtDay
	s += swk * in.WgtWeek
	s += smo * in.WgtMonth
	s /= in.WgtDay + in.WgtWeek + in.WgtMonth
	s = math.Min(100, math.Max(0, s))

	gio.Emit([]float64{s})

	return nil
}

func calcKdjScore(kdj []*model.Indicator, buyfds []*model.KDJfdView, sellfds []*model.KDJfdView) (s float64, e error) {
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
	xs := x.([]float64)
	ys := y.([]float64)
	return append(xs, ys...), nil
}

// Evaluates KDJ DEVIA indicator against pruned feature data, returns the following result:
// Ratio of high DEVIA, ratio of positive DEVIA, mean of positive DEVIA, and DEVIA indicator, ranging from 0 to 1
func calcKdjDI(hist []*model.Indicator, fdvs []*model.KDJfdView) (hdr, pdr, mpd, di float64, e error) {
	if len(hist) == 0 {
		return 0, 0, 0, 0, nil
	}
	code := hist[0].Code
	hk := make([]float64, len(hist))
	hd := make([]float64, len(hist))
	hj := make([]float64, len(hist))
	for i, h := range hist {
		hk[i] = h.KDJ_K
		hd[i] = h.KDJ_D
		hj[i] = h.KDJ_J
	}
	pds := make([]float64, 0, 16)
	for _, fd := range fdvs {
		bkd, e := bestKdjDevi(hk, hd, hj, fd.K, fd.D, fd.J)
		if e != nil {
			return 0, 0, 0, 0, e
		}
		if bkd >= 0 {
			pds = append(pds, bkd)
			pdr += fd.Weight
			if bkd >= 0.8 {
				hdr += fd.Weight
			}
		}
	}
	if len(pds) > 0 {
		mpd, e = stats.Mean(pds)
		e = errors.Wrapf(e, "%s failed to calculate mean of positive devia", code)
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
func bestKdjDevi(sk, sd, sj, tk, td, tj []float64) (float64, error) {
	//should we also consider the len(x) to weigh the final result?
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

func CalcKdjDevi(sk, sd, sj, tk, td, tj []float64) (float64, error) {
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

func Devi(a, b []float64) (float64, error) {
	if len(a) != len(b) || len(a) == 0 {
		return 0, errors.New("invalid input")
	}
	s := .0
	for i := 0; i < len(a); i++ {
		s += math.Pow(a[i]-b[i], 2)
	}
	return math.Pow(s/float64(len(a)), 0.5), nil
}
