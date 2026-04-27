package tradeengine

// TradeState stores trade info at entry time and carries until close.
type TradeState struct {
	SID    string
	Symbol string
	// ... existing fields would go here
	MetaEnforce *struct {
		CovBucket string
		Applied   bool
	}
}
