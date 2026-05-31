package liquidation

import (
	"errors"
	"math/big"
	"strings"
)

// mulDecimalStrings перемножает два десятичных числа (строки) и возвращает строку.
//
// Используем big.Rat вместо float64, чтобы:
//   - избежать накопления ошибок округления
//   - обеспечить детерминизм
//
// Форматируем результат с 8 знаками после запятой.
func mulDecimalStrings(a, b string) (string, error) {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if a == "" || b == "" {
		return "", errors.New("empty operand")
	}
	ra, ok := new(big.Rat).SetString(a)
	if !ok {
		return "", errors.New("bad decimal a")
	}
	rb, ok := new(big.Rat).SetString(b)
	if !ok {
		return "", errors.New("bad decimal b")
	}
	out := new(big.Rat).Mul(ra, rb)
	return out.FloatString(8), nil
}
