package db

import (
	"gopkg.in/gorp.v2"
	"github.com/carusyte/stock/util"
	"database/sql"
	_ "gopkg.in/rana/ora.v4"
	"gopkg.in/couchbase/gocb.v1"
)

var (
	ora    *gorp.DbMap
	cbclus *gocb.Cluster
	inited = false
)

func Ora() *gorp.DbMap {
	if !inited {
		initGorp()
	}
	return ora
}

func initGorp() {
	db, err := sql.Open("ora", "rima/rima@10.16.53.30:1521/hundsun")
	util.CheckErr(err, "sql.Open failed,")

	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(64)

	// construct a gorp DbMap
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.OracleDialect{}}

	util.CheckErr(db.Ping(), "Failed to ping db,")

	ora = dbmap
	inited = true
}

func initCb() {
	var e error
	cbclus, e = gocb.Connect("couchbase://10.16.53.10,10.16.53.11")
	util.CheckErr(e, "failed to connect to couchbase cluster.")
}

// Remember to close the bucket after use.
func Cb() *gocb.Bucket {
	if cbclus == nil {
		initCb()
	}
	bucket, e := cbclus.OpenBucket("rima", "")
	util.CheckErr(e, "failed to open couchbase bucket")
	return bucket
}
