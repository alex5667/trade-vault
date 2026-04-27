import { Injectable } from '@nestjs/common'
import type { Redis } from 'ioredis'

export type TradeEventType = 'POSITION_OPENED' | 'POSITION_CLOSED'

export interface MetaEnforceInfo {
	covBucket: string // e.g. "control" | "enforce" | "enforce:A"
	applied: boolean
}

export interface PositionClosedEvent {
	event_type: 'POSITION_CLOSED'
	ts_ms: number
	symbol: string
	sid: string
	r_mult: number
	// P41:
	meta_enforce_cov_bucket: string
	meta_enforce_applied: boolean
	// aliases (optional):
	meta_cov_bucket?: string
	meta_applied?: boolean
}

function nowMs(): number {
	return Date.now()
}

function sanitizeBucket(v: unknown): string {
	const s = String(v ?? '').trim()
	if (!s) return 'unknown'
	// keep ASCII-ish and short; avoid unbounded cardinality
	return s.slice(0, 64)
}

@Injectable()
export class TradeEventsPublisher {
	private readonly stream = process.env.TRADE_EVENTS_STREAM ?? 'events:trades';
	private readonly maxlen = Number(process.env.TRADE_EVENTS_MAXLEN ?? '200000');

	constructor(private readonly redis: Redis) { }

	async publishPositionClosed(args: {
		symbol: string
		sid: string
		rMult: number
		meta?: MetaEnforceInfo | null
	}): Promise<void> {
		const meta = args.meta ?? { covBucket: 'unknown', applied: false }

		const ev: PositionClosedEvent = {
			event_type: 'POSITION_CLOSED',
			ts_ms: nowMs(),
			symbol: String(args.symbol).toUpperCase(),
			sid: String(args.sid),
			r_mult: Number(args.rMult),
			meta_enforce_cov_bucket: sanitizeBucket(meta.covBucket),
			meta_enforce_applied: Boolean(meta.applied),
			// aliases for consumers during rollout:
			meta_cov_bucket: sanitizeBucket(meta.covBucket),
			meta_applied: Boolean(meta.applied),
		}

		// Prefer a single 'payload' field to keep XADD simple and consistent with your Python consumers.
		const fields: Record<string, string> = {
			payload: JSON.stringify(ev),
		}

		await this.redis.xadd(this.stream, 'MAXLEN', '~', this.maxlen, '*', ...Object.entries(fields).flat())
	}
}
