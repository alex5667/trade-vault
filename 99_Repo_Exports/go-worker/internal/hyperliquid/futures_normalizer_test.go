package hyperliquid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeFuturesMessage_Trades(t *testing.T) {
	raw := []byte(`{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"65000","sz":"0.01","hash":"0xabc","time":1700000000123,"tid":12345,"users":["0x1","0x2"]}]}`)

	ticks, books, err := NormalizeFuturesMessage(raw, map[string]string{}, "USDT", 20)
	require.NoError(t, err)
	require.Len(t, ticks, 1)
	require.Len(t, books, 0)

	tk := ticks[0]
	require.Equal(t, "BTCUSDT", tk.Symbol)
	require.Equal(t, int64(1700000000123), tk.Ts)
	require.Equal(t, "BUY", tk.Side)
	require.Equal(t, int64(12345), tk.TradeID)
	require.Equal(t, "hyperliquid", tk.Source)
}

func TestNormalizeFuturesMessage_L2Book(t *testing.T) {
	raw := []byte(`{"channel":"l2Book","data":{"coin":"ETH","levels":[[{"px":"3500.0","sz":"1.2","n":2}],[{"px":"3500.5","sz":"0.8","n":1}]],"time":1700000000456}}`)

	ticks, books, err := NormalizeFuturesMessage(raw, map[string]string{}, "USDT", 20)
	require.NoError(t, err)
	require.Len(t, ticks, 0)
	require.Len(t, books, 1)

	bk := books[0]
	require.Equal(t, "ETHUSDT", bk.Symbol)
	require.Equal(t, int64(1700000000456), bk.Ts)
	require.Len(t, bk.Bids, 1)
	require.Equal(t, "3500.0", bk.Bids[0][0])
	require.Equal(t, "1.2", bk.Bids[0][1])
	require.Len(t, bk.Asks, 1)
}

func TestNormalizeEpochMs_SecondsInput(t *testing.T) {
	// 1700000000 looks like seconds, should become ms.
	raw := []byte(`{"channel":"trades","data":[{"coin":"BTC","side":"A","px":"1","sz":"1","hash":"h","time":1700000000,"tid":1,"users":["a","b"]}]}`)
	ticks, _, err := NormalizeFuturesMessage(raw, map[string]string{}, "USDT", 20)
	require.NoError(t, err)
	require.Len(t, ticks, 1)
	require.Equal(t, int64(1700000000*1000), ticks[0].Ts)
	require.Equal(t, "SELL", ticks[0].Side)
}

func TestNormalizeSymbol_MapOverride(t *testing.T) {
	raw := []byte(`{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"1","hash":"h","time":1700000000123,"tid":1,"users":["a","b"]}]}`)
	m := map[string]string{"BTC": "XBTUSD"}
	ticks, _, err := NormalizeFuturesMessage(raw, m, "USDT", 20)
	require.NoError(t, err)
	require.Equal(t, "XBTUSD", ticks[0].Symbol)
}
