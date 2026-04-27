import { Injectable } from '@nestjs/common'
import { TradeEventsPublisher } from '../trade-events/trade-events.publisher'
import { TradeState } from './trade-state'

@Injectable()
export class TradeEngineService {
	constructor(private readonly tradeEventsPublisher: TradeEventsPublisher) { }

	async onPositionClosed(trade: TradeState, rMult: number) {
		// IMPORTANT: use stored meta from entry time, do NOT recompute on close.
		const meta = trade.metaEnforce ?? { covBucket: 'unknown', applied: false }
		await this.tradeEventsPublisher.publishPositionClosed({
			symbol: trade.symbol,
			sid: trade.sid,
			rMult,
			meta,
		})
	}
}
