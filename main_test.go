package main

import (
	"testing"
	"time"
	"fmt"
)

func TestTimeUnix(t *testing.T) {
	sec := 1504627200000 / int64(time.Microsecond)
	tm := time.Unix(sec,0)
	fmt.Println(tm)
}
