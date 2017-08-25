package cache

import (
	"gopkg.in/couchbase/gocb.v1"
	"github.com/carusyte/stock/util"
)

var(
	cbclus *gocb.Cluster
)

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