package cache

import (
	"gopkg.in/couchbase/gocb.v1"
	"github.com/carusyte/rima/conf"
	"time"
	"math/rand"
	"strings"
	"github.com/sirupsen/logrus"
	"github.com/pkg/errors"
	"fmt"
	"log"
	"math"
)

var (
	//cbclus *gocb.Cluster
	bucket *gocb.Bucket
)

func init() {
	cbclus, e := gocb.Connect(fmt.Sprintf("couchbase://%s", conf.Args.Couchbase.Servers))
	if e != nil {
		log.Panicln("failed to connect to couchbase cluster.", e)
	}
	cbclus.SetEnhancedErrors(true)
	bucket, e = cbclus.OpenBucket(conf.Args.Couchbase.Bucket, "")
	if e != nil {
		log.Panicln("failed to open couchbase bucket", e)
	}
	timeout := time.Second * time.Duration(conf.Args.Couchbase.Timeout)
	bucket.SetOperationTimeout(timeout)
	bucket.SetDurabilityTimeout(timeout)
	bucket.SetViewTimeout(timeout)
	bucket.SetBulkOperationTimeout(timeout)
	bucket.SetDurabilityPollTimeout(timeout)
	bucket.SetN1qlTimeout(timeout)
}

// No need to close the bucket after use.
// According to the documentation,
// https://developer.couchbase.com/documentation/server/4.5/sdk/go/async-programming.html
// numerous asynchronous goroutines can all perform operations on the same Bucket and Cluster objects
func Cb() *gocb.Bucket {
	return bucket
}

// Get value using load balance. Parameter value is usually a pointer.
func GetLB(key string, value interface{}) (e error) {
	bg := time.Now()
	cb := Cb()
	//defer cb.Close()
	numSrv := strings.Count(conf.Args.Couchbase.Servers, ",") + 1
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
	//defer b.Close()
	_, err := b.MapRemove(key, path)
	return err
}

func RemoveElements(key string, paths []string, failfast bool) (e error) {
	if len(paths) == 0 {
		return nil
	}
	//defer b.Close()
	mib := Cb().MutateIn(key, 0, 0)
	for i, p := range paths {
		mib.Remove(p)
		if (i != 0 && i%(conf.Args.Couchbase.MutationOpSize-1) == 0) || i == len(paths)-1 {
			_, e := mib.Execute()
			if e != nil {
				st := int(math.Max(0, float64(i-conf.Args.Couchbase.MutationOpSize+1)))
				if failfast {
					return errors.Wrapf(e, "key [%s] failed to remove %+v", key, paths[st:i])
				} else {
					logrus.Errorf("key [%s] failed to remove %+v", key, paths[st:i])
				}
			}
			mib = Cb().MutateIn(key, 0, 0)
		}
	}
	return
}

func UpsertElement(key, path string, value interface{}) error {
	b := Cb()
	//defer b.Close()
	_, err := b.MutateIn(key, 0, 0).Upsert(path, value, false).Execute()
	return err
}

func UpsertElements(key string, elements map[string]interface{}, failfast bool) (e error) {
	if len(elements) == 0 {
		return nil
	}
	mib := Cb().MutateIn(key, 0, 0)
	//defer b.Close()
	pend := make(map[string]interface{})
	for p, v := range elements {
		pend[p] = v
		i := len(pend)
		mib.Upsert(p, v, false)
		if (i != 0 && i%(conf.Args.Couchbase.MutationOpSize-1) == 0) || i == len(elements)-1 {
			_, e := mib.Execute()
			if e != nil {
				if failfast {
					return errors.Wrapf(e, "key [%s] failed to upsert %+v", key, pend)
				} else {
					logrus.Errorf("key [%s] failed to upsert %+v", key, pend)
				}
			}
			mib = Cb().MutateIn(key, 0, 0)
			pend = make(map[string]interface{})
		}
	}
	return e
}
