package calendar

import "testing"

func TestParseFMPDateUTC(t *testing.T) {
	// 2024-12-01 13:30:00 should parse
	ms := parseFMPDateUTC("2024-12-01 13:30:00")
	if ms == 0 {
		t.Fatalf("expected parsed ms")
	}
	// Invalid => 0
	if parseFMPDateUTC("not-a-date") != 0 {
		t.Fatalf("expected 0 on invalid date")
	}
}

func TestCalendarUIDStable(t *testing.T) {
	uid1 := hashUID("fmp", "CPI", "US", "USD", "2024-12-01 13:30:00")
	uid2 := hashUID("fmp", "CPI", "US", "USD", "2024-12-01 13:30:00")
	if uid1 != uid2 {
		t.Fatalf("expected stable uid")
	}
	if uid1 == hashUID("fmp", "CPI", "US", "EUR", "2024-12-01 13:30:00") {
		t.Fatalf("expected different uid for different currency")
	}
}

func TestImportanceToInt(t *testing.T) {
	if importanceToInt("High") != 3 { t.Fatalf("High") }
	if importanceToInt("Medium") != 2 { t.Fatalf("Medium") }
	if importanceToInt("Low") != 1 { t.Fatalf("Low") }
	if importanceToInt("") != 0 { t.Fatalf("empty") }
}
