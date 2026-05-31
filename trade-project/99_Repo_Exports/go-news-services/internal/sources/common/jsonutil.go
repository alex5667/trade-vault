package common

import (
	"encoding/json"
)

func MustJSON(v any, fallback string) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fallback
	}
	return string(b)
}

func JSONArrayStrings(xs []string) string {
	if xs == nil {
		xs = []string{}
	}
	return MustJSON(xs, "[]")
}

func JSONObject(v any) string {
	return MustJSON(v, "{}")
}
