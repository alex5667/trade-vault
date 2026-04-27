export interface TradeState {
	sid: string
	symbol: string
	// ... existing fields would go here
	metaEnforce?: {
		covBucket: string
		applied: boolean
	}
}
