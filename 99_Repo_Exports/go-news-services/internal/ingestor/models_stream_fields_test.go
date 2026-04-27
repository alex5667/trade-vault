package ingestor

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func canon(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, " ", "")
	return s
}

func keyForField(fields map[string]any, fieldName string) (string, bool) {
	// Для ToStreamFields имена ключей могут отличаться от имен полей структуры
	// Проверим несколько возможных соответствий
	possibleKeys := []string{fieldName, strings.ToLower(fieldName)}
	for _, key := range possibleKeys {
		if _, ok := fields[key]; ok {
			return key, true
		}
	}
	// Специальные соответствия
	switch fieldName {
	case "SymbolsJSON":
		if _, ok := fields["symbols"]; ok {
			return "symbols", true
		}
	case "PayloadJSON":
		if _, ok := fields["payload"]; ok {
			return "payload", true
		}
	case "PublishedTSms":
		if _, ok := fields["published_ts_ms"]; ok {
			return "published_ts_ms", true
		}
	case "IngestedTSms":
		if _, ok := fields["ingested_ts_ms"]; ok {
			return "ingested_ts_ms", true
		}
	case "EventTSms":
		if _, ok := fields["event_ts_ms"]; ok {
			return "event_ts_ms", true
		}
	}
	return "", false
}

func assertAllValuesAreStrings(t *testing.T, fields map[string]any) {
	t.Helper()
	for k, v := range fields {
		if _, ok := v.(string); !ok {
			t.Fatalf("ToStreamFields value must be string: key=%q type=%T", k, v)
		}
	}
}

func assertStructFieldsCoveredAndParsable(t *testing.T, st any, fields map[string]any) {
	t.Helper()

	rt := reflect.TypeOf(st)
	if rt.Kind() != reflect.Struct {
		t.Fatalf("expected struct, got %v", rt.Kind())
	}

	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		// только exported
		if f.PkgPath != "" {
			continue
		}

		k, ok := keyForField(fields, f.Name)
		if !ok {
			t.Fatalf("missing field in ToStreamFields(): struct_field=%q", f.Name)
		}

		s := fields[k].(string)
		switch f.Type.Kind() {
		case reflect.Int, reflect.Int64, reflect.Int32:
			if _, err := strconv.ParseInt(s, 10, 64); err != nil {
				t.Fatalf("field %q should be int string, got %q err=%v", f.Name, s, err)
			}
		case reflect.Float64, reflect.Float32:
			if _, err := strconv.ParseFloat(s, 64); err != nil {
				t.Fatalf("field %q should be float string, got %q err=%v", f.Name, s, err)
			}
		default:
			// string поля просто должны быть строкой; пусто — допустимо
		}
	}
}

func TestNewsRawItem_ToStreamFields_Contract(t *testing.T) {
	n := NewsRawItem{
		UID:           "123456789012345678901234", // 24
		PublishedTSms: 1700000000000,
		IngestedTSms:  1700000001000,
		Source:        "x",
		Title:         "t",
		URL:           "u",
		Summary:       "s",
		SymbolsJSON:   `["BTC"]`,
		Importance:    0.5,
		PayloadJSON:   `{"k":"v"}`,
	}

	m := n.ToStreamFields()

	if len(m) == 0 {
		t.Fatal("ToStreamFields() returned empty map")
	}
	assertAllValuesAreStrings(t, m)
	assertStructFieldsCoveredAndParsable(t, n, m)

	// Дополнительная проверка: uid действительно 24 символа (как ваш StableUID)
	if k, ok := keyForField(m, "UID"); ok {
		if len(m[k].(string)) != 24 {
			t.Fatalf("UID length must be 24, got %d", len(m[k].(string)))
		}
	}
}

func TestCalendarEvent_ToStreamFields_Contract(t *testing.T) {
	c := CalendarEvent{
		UID:          "123456789012345678901234",
		EventTSms:    1700000000000,
		IngestedTSms: 1700000001000,
		Country:      "US",
		Currency:     "USD",
		Title:        "NFP",
		Importance:   3,
		Forecast:     "1.0",
		Previous:     "0.9",
		Unit:         "%",
		Source:       "fmp",
		PayloadJSON:  `{"k":"v"}`,
	}

	m := c.ToStreamFields()

	if len(m) == 0 {
		t.Fatal("ToStreamFields() returned empty map")
	}
	assertAllValuesAreStrings(t, m)
	assertStructFieldsCoveredAndParsable(t, c, m)
}
