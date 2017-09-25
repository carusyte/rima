package main

import (
	"testing"
	"time"
	"fmt"
	"google.golang.org/grpc"
)

func TestTimeUnix(t *testing.T) {
	sec := 1504627200000 / int64(time.Microsecond)
	tm := time.Unix(sec, 0)
	fmt.Println(tm)
}

func TestGrpd(t *testing.T) {
	c,e:=grpc.Dial("adsfasdf", grpc.WithInsecure())
	if e!=nil{
		panic(e)
	}
	fmt.Printf("%+v",c.GetState())
}
