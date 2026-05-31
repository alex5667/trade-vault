/**
 * Reference-only P4.1 adapter for external NestJS WS gateway.
 * Writes unified latency contract hashes for service=nest_gateway stages:
 *  - emit_to_ws
 *  - end_to_end_event
 *
 * Copy this snippet into your NestJS gateway service; adapt as needed.
 * The nest_gateway owns end_to_end_event because only it knows ts_ws_emit_ms.
 */
import Redis from 'ioredis'

export type LatencyWsPayload = {
	symbol: string
	ts_event_ms: number
	ts_emit_ms: number
	ts_ws_emit_ms: number
	instance_id?: string
	source?: string
}

function upperSymbol(symbol: string): string {
	return String(symbol || '').trim().toUpperCase()
}

/**
 * writeNestGatewayLatency writes both emit_to_ws and end_to_end_event hashes
 * to Redis. Call this from the WS emit path after you have ts_ws_emit_ms.
 *
 * @param redis    ioredis client
 * @param keyPrefix  e.g. "metrics:latency_contract:last"
 * @param ttlSec   TTL in seconds (default: 172800 = 48h)
 * @param payload  timing fields for one symbol's WS emit event
 */
export async function writeNestGatewayLatency(
	redis: Redis,
	keyPrefix: string,
	ttlSec: number,
	payload: LatencyWsPayload,
): Promise<void> {
	const symbol = upperSymbol(payload.symbol)
	if (!symbol) return
	const nowMs = Date.now()
	const emitToWs = Math.max(0, Number(payload.ts_ws_emit_ms) - Number(payload.ts_emit_ms))
	const endToEnd = Math.max(0, Number(payload.ts_ws_emit_ms) - Number(payload.ts_event_ms))
	const common: Record<string, string> = {
		schema_version: '1',
		service: 'nest_gateway',
		symbol,
		last_ts_ms: String(nowMs),
		ts_event_ms: String(payload.ts_event_ms || 0),
		ts_emit_ms: String(payload.ts_emit_ms || 0),
		ts_ws_emit_ms: String(payload.ts_ws_emit_ms || 0),
		instance_id: String(payload.instance_id || ''),
		source: String(payload.source || 'nest_gateway_example'),
	}
	const emitKey = `${keyPrefix}:nest_gateway:emit_to_ws:${symbol}`
	const endKey = `${keyPrefix}:nest_gateway:end_to_end_event:${symbol}`
	await redis.hset(emitKey, { ...common, stage: 'emit_to_ws', last_duration_ms: String(emitToWs) })
	await redis.expire(emitKey, ttlSec)
	await redis.hset(endKey, { ...common, stage: 'end_to_end_event', last_duration_ms: String(endToEnd) })
	await redis.expire(endKey, ttlSec)
}
