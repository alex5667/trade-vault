export type RuntimeSnapshot = {
	symbol: string
	balance: number
	atr?: number
	pivots?: { H: number; L: number; C: number; day?: string }
	dom?: {
		ts: number
		symbol: string
		provider: string
		mid: number
		bids: [number, number][]
		asks: [number, number][]
	}
}

const BASE = process.env.GATEWAY_URL ?? "http://localhost:8090"

export async function getRuntimeSnapshot(): Promise<RuntimeSnapshot> {
	const r = await fetch(`${BASE}/runtime/snapshot`, { cache: "no-store" })
	if (!r.ok) throw new Error(`snapshot http ${r.status}`)
	return r.json()
}

export type PushOrderPayload = {
	action: "open"
	symbol: string
	side: "LONG" | "SHORT"
	lot: number
	sl: number
	tp_levels: number[]
	sid?: string
	entry?: number
}

export async function pushOrder(p: PushOrderPayload) {
	const r = await fetch(`${BASE}/orders/push`, {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify(p),
	})
	if (!r.ok) throw new Error(`push http ${r.status}`)
	try {
		return await r.json()
	} catch {
		return { ok: true } as const
	}
}


