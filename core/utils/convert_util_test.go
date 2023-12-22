package utils

import (
	"testing"
)

func TestTs(t *testing.T) {
	ts := 446042051352461313
	time, logical := ParseTS(uint64(ts))
	println(time.Unix())
	println(logical)
}
