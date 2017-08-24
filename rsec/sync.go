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

func (d *DataSync) SyncKdjFd(req *map[string][]*model.KDJfdView, rep *bool) error {
	st := time.Now()
	fdMap := *req
	log.Printf("DataSync.SyncKdjFd called, input size: %d", len(fdMap))
	defer func() {
		log.Printf("DataSync.SyncKdjFd finished, input size: %d, time: %.2f", len(fdMap), time.Since(st).Seconds())
	}()
	tran, e := db.Ora().Begin()
	if e != nil {
		log.Println("failed to start new transaction", e)
		return errors.Wrap(e, "failed to start new transaction")
	}
	_, e = tran.Exec("truncate table indc_feat")
	if e != nil {
		log.Println("failed to truncate indc_feat", e)
		return errors.Wrap(e, "failed to truncate indc_feat")
	}
	_, e = tran.Exec("truncate table kdj_feat_dat")
	if e != nil {
		log.Println("failed to truncate kdj_feat_dat", e)
		return errors.Wrap(e, "failed to truncate kdj_feat_dat")
	}
	e = tran.Commit()
	if e != nil {
		log.Println("failed to commit transaction", e)
		return errors.Wrap(e, "failed to commit transaction")
	}
	for k, fdvs := range fdMap {
		log.Printf("Saving KDJ Feature Data, key: %s,  len: %d", k, len(fdvs))
		e := bulkInsertFdvs(fdvs)
		if e != nil {
			return e
		}
	}
	*rep = true
	return nil
}

func bulkInsertFdvs(fdvs []*model.KDJfdView) error {
	if len(fdvs) <= 0 {
		return nil
	}
	tran, e := db.Ora().Begin()
	if e != nil {
		log.Println("failed to start new transaction", e)
		return errors.Wrap(e, "failed to start new transaction")
	}
	valueStrings := make([]string, 0, len(fdvs))
	valueArgs := make([]interface{}, 0, len(fdvs)*10)
	dt, tm := util.TimeStr()
	for i, f := range fdvs {
		valueString := fmt.Sprintf(" SELECT :indc%[1]d,:fid%[1]d,:cytp%[1]d,"+
			":bysl%[1]d,:smpnum%[1]d,:fdnum%[1]d,:weight%[1]d,:remarks%[1]d,:dt%[1]d,:tm%[1]d "+
			"FROM dual", i)
		if i < len(fdvs)-1 {
			valueString += " UNION ALL "
		}
		valueStrings = append(valueStrings, valueString)
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
		"udate,utime) WITH t AS (%s) SELECT * FROM t",
		strings.Join(valueStrings, ""))
	ps, e := tran.Prepare(stmt)
	if e != nil {
		tran.Rollback()
		log.Println("failed to create indc_feat prepared statement", e)
		return errors.Wrap(e, "failed to create indc_feat prepared statement")
	}
	_, err := ps.Exec(valueArgs...)
	if err != nil {
		tran.Rollback()
		log.Println("failed to bulk insert indc_feat", err)
		return errors.Wrap(err, "failed to bulk insert indc_feat")
	}

	for _, f := range fdvs {
		valueStrings = make([]string, 0, f.SmpNum)
		valueArgs = make([]interface{}, 0, f.SmpNum*7)
		for i := 0; i < f.SmpNum; i++ {
			valueString := fmt.Sprintf(" SELECT :fid%[1]d,:seq%[1]d,:k%[1]d,"+
				":d%[1]d,:j%[1]d,:dt%[1]d,:tm%[1]d FROM dual", i)
			if i < f.SmpNum-1 {
				valueString += " UNION ALL "
			}
			valueStrings = append(valueStrings, valueString)
			valueArgs = append(valueArgs, f.Fid)
			valueArgs = append(valueArgs, i)
			valueArgs = append(valueArgs, f.K[i])
			valueArgs = append(valueArgs, f.D[i])
			valueArgs = append(valueArgs, f.J[i])
			valueArgs = append(valueArgs, dt)
			valueArgs = append(valueArgs, tm)
		}
		stmt = fmt.Sprintf("INSERT INTO kdj_feat_dat (fid,seq,k,d,j,"+
			"udate,utime) WITH t AS (%s) SELECT * FROM t",
			strings.Join(valueStrings, ""))
		ps, e := tran.Prepare(stmt)
		if e != nil {
			tran.Rollback()
			log.Println("failed to create kdj_feat_dat prepared statement", e)
			return errors.Wrap(e, "failed to create kdj_feat_dat prepared statement")
		}
		_, err := ps.Exec(valueArgs...)
		if err != nil {
			tran.Rollback()
			log.Println("failed to bulk insert kdj_feat_dat", err)
			return errors.Wrap(err, "failed to bulk insert kdj_feat_dat")
		}
	}
	e = tran.Commit()
	if e != nil {
		log.Println("failed to commit transaction", e)
		return errors.Wrap(e, "failed to commit transaction")
	}
	return nil
}
