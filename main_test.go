package main

import (
	"testing"
	"time"
	"fmt"
	"google.golang.org/grpc"
	"context"
)

func TestTimeUnix(t *testing.T) {
	sec := 1504627200000 / int64(time.Microsecond)
	tm := time.Unix(sec, 0)
	fmt.Println(tm)
}

func TestGrpd(t *testing.T) {
	ctx := context.Background()
	tctx, cancel := context.WithTimeout(ctx, time.Millisecond*10000)
	defer cancel()
	c, e := grpc.DialContext(tctx, "adsfasdf", grpc.WithInsecure(), grpc.WithBlock())
	if e != nil {
		panic(e)
	}
	fmt.Printf("%+v", c.GetState())
}

func TestBoolSliceInit(t *testing.T) {
	bs := make([]bool, 10)
	fmt.Printf("%+v", bs)
}

func TestGoroutine(t *testing.T) {
	go func() {
		time.Sleep(time.Second * 3)
		fmt.Println("goroutine executed")
	}()
	fmt.Println("test func exiting")
	time.Sleep(time.Second * 4)
}
