package ingestor

import (
	"strconv"
	"time"
)

// BucketStartMs returns bucket "floor" start timestamp in ms for a given bucket duration.
// Example: ts=10:17, bucket=6h => start=06:00 boundary in ms.
func BucketStartMs(tsMs int64, bucket time.Duration) int64 {
	if tsMs <= 0 || bucket <= 0 {
		return 0
	}
	bms := bucket.Milliseconds()
	if bms <= 0 {
		return 0
	}
	return (tsMs / bms) * bms
}

func BucketKey(tsMs int64, bucket time.Duration) string {
	return strconv.FormatInt(BucketStartMs(tsMs, bucket), 10)
}

// BucketStartMsOrZero:
// - если published timestamp невалидный (<=0) -> 0,
//   чтобы UID не "прыгал" по границам бакета и не проходил дедуп снова.
// - иначе обычный floor по границе бакета.
func BucketStartMsOrZero(tsMs int64, bucket time.Duration) int64 {
	if tsMs <= 0 || bucket <= 0 {
		return 0
	}
	return BucketStartMs(tsMs, bucket)
}
