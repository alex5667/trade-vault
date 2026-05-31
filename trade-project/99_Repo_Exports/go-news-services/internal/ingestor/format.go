package ingestor

import "strconv"

func itoa64(v int64) string { return strconv.FormatInt(v, 10) }
func itoa(v int64) string   { return strconv.FormatInt(v, 10) }
func ftoa(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }
