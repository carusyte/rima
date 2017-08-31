package cache

import (
	"gopkg.in/couchbase/gocb.v1"
	"fmt"
	"github.com/carusyte/rima/conf"
	"log"
)

var (
	cbclus *gocb.Cluster
)

func initCb() {
	var e error
	cbclus, e = gocb.Connect(fmt.Sprintf("couchbase://%s", conf.Args.CouchbaseServers))
	if e != nil {
		log.Panicln("failed to connect to couchbase cluster.", e)
	}
}

// Remember to close the bucket after use.
func Cb() *gocb.Bucket {
	if cbclus == nil {
		initCb()
	}
	bucket, e := cbclus.OpenBucket("rima", "")
	if e != nil {
		log.Panicln("failed to open couchbase bucket", e)
	}
	return bucket
}
