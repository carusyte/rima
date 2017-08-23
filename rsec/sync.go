package rsec

import (
	"github.com/carusyte/stock/model"
	"fmt"
	"github.com/carusyte/stock/util"
	"github.com/carusyte/rima/db"
	"strings"
	"log"
	"time"
	"github.com/pkg/errors"
)

type DataSync struct{}

func (d *DataSync) SyncKdjFd(req *[]*model.KDJfdView, rep *bool) error {
	st := time.Now()
	fdvs := *req
	log.Printf("DataSync.SyncKdjFd called, input size: %d", len(fdvs))
	defer func() {
		log.Printf("DataSync.SyncKdjFd finished, input size: %d, time: %.2f", len(fdvs), time.Since(st).Seconds())
	}()
	tran, e := db.Ora.Begin()
	if e != nil {
		return errors.Wrap(e, "failed to begin new transaction")
	}
	if len(fdvs) > 0 {
		valueStrings := make([]string, 0, len(fdvs))
		valueArgs := make([]interface{}, 0, len(fdvs)*10)
		dt, tm := util.TimeStr()
		for _, f := range fdvs {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, f.Indc)
			valueArgs = append(valueArgs, f.Fid)
			valueArgs = append(valueArgs, f.Cytp)
			valueArgs = append(valueArgs, f.Bysl)
			valueArgs = append(valueArgs, f.SmpNum)
			valueArgs = append(valueArgs, f.FdNum)
			valueArgs = append(valueArgs, f.Weight)
			valueArgs = append(valueArgs, f.Remarks)
			valueArgs = append(valueArgs, dt)
			valueArgs = append(valueArgs, tm)
		}
		stmt := fmt.Sprintf("INSERT INTO indc_feat (indc,fid,cytp,bysl,smp_num,fd_num,weight,remarks,"+
			"udate,utime) VALUES %s on duplicate key update fid=values(fid),fd_num=values(fd_num),weight=values"+
			"(weight),remarks=values(remarks),udate=values(udate),utime=values(utime)",
			strings.Join(valueStrings, ","))
		_, err := tran.Exec(stmt, valueArgs...)
		if err != nil {
			tran.Rollback()
			return errors.Wrap(err, "failed to bulk insert indc_feat")
		}

		for _, f := range fdvs {
			valueStrings = make([]string, 0, f.SmpNum)
			valueArgs = make([]interface{}, 0, f.SmpNum*7)
			for i := 0; i < f.SmpNum; i++ {
				valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?)")
				valueArgs = append(valueArgs, f.Fid)
				valueArgs = append(valueArgs, i)
				valueArgs = append(valueArgs, f.K[i])
				valueArgs = append(valueArgs, f.D[i])
				valueArgs = append(valueArgs, f.J[i])
				valueArgs = append(valueArgs, dt)
				valueArgs = append(valueArgs, tm)
			}
			stmt = fmt.Sprintf("INSERT INTO kdj_feat_dat (fid,seq,k,d,j,"+
				"udate,utime) VALUES %s on duplicate key update k=values(k),d=values(d),"+
				"j=values(j),udate=values(udate),utime=values(utime)",
				strings.Join(valueStrings, ","))
			_, err = tran.Exec(stmt, valueArgs...)
			if err != nil {
				tran.Rollback()
				return errors.Wrap(err, "failed to bulk insert kdj_feat_dat")
			}
		}
		tran.Commit()
	}
	*rep = true
	return nil
}
