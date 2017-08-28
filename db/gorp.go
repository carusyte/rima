package db

import (
	"gopkg.in/gorp.v2"
	"github.com/carusyte/stock/util"
	"database/sql"
	//_ "gopkg.in/rana/ora.v4"
	"github.com/carusyte/stock/db"
)

var (
	ora    *gorp.DbMap
	inited = false
)

func Ora() *gorp.DbMap {
	if !inited {
		initGorp()
	}
	return ora
}

func initGorp() {
	//db, err := sql.Open("ora", "rima/rima@10.16.53.30:1521/hundsun")
	//util.CheckErr(err, "sql.Open failed,")
	//
	//db.SetMaxOpenConns(64)
	//db.SetMaxIdleConns(64)
	//
	//// construct a gorp DbMap
	//dbmap := &gorp.DbMap{Db: db, Dialect: gorp.OracleDialect{}}
	//
	//util.CheckErr(db.Ping(), "Failed to ping db,")
	//
	//ora = dbmap
	//inited = true
}
