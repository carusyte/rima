package cache

import (
	"gopkg.in/couchbase/gocb.v1"
	"fmt"
	"github.com/carusyte/rima/conf"
	"log"
	"time"
	"math/rand"
	"strings"
	"github.com/sirupsen/logrus"
	"github.com/pkg/errors"
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
	cbclus.SetEnhancedErrors(true)
	bucket, e := cbclus.OpenBucket("rima", "")
	if e != nil {
		log.Panicln("failed to open couchbase bucket", e)
	}
	timeout := time.Second * time.Duration(conf.Args.CouchbaseTimeout)
	bucket.SetOperationTimeout(timeout)
	bucket.SetDurabilityTimeout(timeout)
	bucket.SetViewTimeout(timeout)
	bucket.SetBulkOperationTimeout(timeout)
	bucket.SetDurabilityPollTimeout(timeout)
	bucket.SetN1qlTimeout(timeout)
	return bucket
}

// Get value using load balance. Parameter value is usually a pointer.
func GetLB(key string, value interface{}) (e error) {
	bg := time.Now()
	cb := Cb()
	defer cb.Close()
	numSrv := strings.Count(conf.Args.CouchbaseServers, ",") + 1
	switch rand.Intn(numSrv) {
	case 0:
		_, e = cb.Get(key, value)
		if e == nil {
			logrus.Debugf("[%s] get data from couchbase, time elapsed: %.2f",
				key, time.Since(bg).Seconds())
		} else {
			logrus.Errorf("[%s] failed to get data from couchbase primary server, time elapsed: %.2f\n "+
				"%+v\n retry with replica server", key, time.Since(bg).Seconds(), e)
			bg = time.Now()
			_, e = cb.GetReplica(key, value, 0)
			if e != nil {
				logrus.Errorf("[%s] failed to get data from couchbase replica, time elapsed: %.2f\n "+
					"%+v\n", key, time.Since(bg).Seconds(), e)
				return errors.Wrapf(e, "[%s] failed to get data from cache server", key)
			}
		}
	default:
		_, e = cb.GetReplica(key, value, 0)
		if e == nil {
			logrus.Debugf("[%s] get data from couchbase, time elapsed: %.2f",
				key, time.Since(bg).Seconds())
		} else {
			logrus.Errorf("[%s] failed to get data from couchbase replica, time elapsed: %.2f\n "+
				"%+v\n retry with primary server", key, time.Since(bg).Seconds(), e)
			bg = time.Now()
			_, e = cb.Get(key, value)
			if e != nil {
				logrus.Errorf("[%s] failed to get data from couchbase primary server, time elapsed: %.2f\n "+
					"%+v\n", key, time.Since(bg).Seconds(), e)
				return errors.Wrapf(e, "[%s] failed to get data from cache server", key)
			}
		}
	}
	return nil
}

func RemoveElement(key, path string) error {
	b := Cb()
	defer b.Close()
	_, err := b.MapRemove(key, path)
	return err
}

func UpsertElement(key, path string, value interface{}) error {
	b := Cb()
	defer b.Close()
	_, err := b.MutateIn(key, 0, 0).Upsert(path, value, false).Execute()
	return err
}
