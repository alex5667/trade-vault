package liquidation

import "testing"

func TestMulDecimalStrings(t *testing.T) {
	out, err := mulDecimalStrings("50000", "0.08")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// 50000 * 0.08 = 4000
	if out != "4000.00000000" {
		t.Fatalf("unexpected out: %s", out)
	}
}
