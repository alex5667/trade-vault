/**
 * bot-nest/src/contracts/horizon-meta-v2.ts
 *
 * Phase 0: Horizon-aware контракт для NestJS / TypeScript consumers.
 *
 * Совместимость:
 *   - SignalMetaV2 расширяет legacy meta (sl_mode, sl_atr_mult, regime, dq_flags, ml_confirm_p).
 *   - Новые поля horizon и atr_profile — optional (backward compat для старых producers).
 *   - contract_ver=2 является маркером нового контракта.
 *
 * ВАЖНО:
 *   - Новые поля НЕ входят в signal_id base (dedup/replay детерминизм сохранён).
 *   - consumers, читающие только sl_mode/sl_atr_mult, работают без изменений.
 */

// ─── Literal Types ─────────────────────────────────────────────────────────────

export type PhaseMode = 'off' | 'shadow' | 'canary' | 'enforce'
export type AtrMode = 'legacy' | 'horizon'
export type HorizonBucket = 'micro' | 'short' | 'medium' | 'long' | 'unknown'
export type AtrSource = 'legacy' | 'bootstrap' | 'selector' | 'manual' | 'fallback' | 'unknown'

// ─── Horizon Reason Codes ─────────────────────────────────────────────────────

export const HorizonReasonCode = {
  // Horizon
  HZ_OK: 'HZ_OK',
  HZ_STATIC_BOOTSTRAP: 'HZ_STATIC_BOOTSTRAP',
  HZ_HISTORY_PROFILE: 'HZ_HISTORY_PROFILE',
  HZ_FALLBACK_UNKNOWN_KIND: 'HZ_FALLBACK_UNKNOWN_KIND',
  HZ_FALLBACK_UNKNOWN_REGIME: 'HZ_FALLBACK_UNKNOWN_REGIME',
  HZ_LOW_SAMPLE_PROFILE: 'HZ_LOW_SAMPLE_PROFILE',
  HZ_MISSING_PROFILE: 'HZ_MISSING_PROFILE',
  HZ_PROFILE_STALE: 'HZ_PROFILE_STALE',
  HZ_MAX_SIGNAL_AGE_EXCEEDED: 'HZ_MAX_SIGNAL_AGE_EXCEEDED',
  // ATR
  ATR_OK: 'ATR_OK',
  ATR_LEGACY_ALIAS: 'ATR_LEGACY_ALIAS',
  ATR_SELECTOR_PENDING: 'ATR_SELECTOR_PENDING',
  ATR_PROFILE_MISSING: 'ATR_PROFILE_MISSING',
  ATR_PROFILE_STALE: 'ATR_PROFILE_STALE',
  ATR_HORIZON_MISMATCH: 'ATR_HORIZON_MISMATCH',
  ATR_SOURCE_FALLBACK: 'ATR_SOURCE_FALLBACK',
  ATR_WINDOW_INVALID: 'ATR_WINDOW_INVALID',
  ATR_TF_UNSUPPORTED: 'ATR_TF_UNSUPPORTED',
  // DQ future-reserved
  DQ_BOOK_STALE_FOR_HORIZON: 'DQ_BOOK_STALE_FOR_HORIZON',
  DQ_ATR_STALE_FOR_HORIZON: 'DQ_ATR_STALE_FOR_HORIZON',
  DQ_ATR_UNAVAILABLE: 'DQ_ATR_UNAVAILABLE',
  DQ_TICK_GAP_CRITICAL: 'DQ_TICK_GAP_CRITICAL',
  DQ_SIGNAL_TOO_OLD: 'DQ_SIGNAL_TOO_OLD',
} as const

// ─── DTOs ─────────────────────────────────────────────────────────────────────

/** Horizon profile snapshot at signal decision time. */
export interface HorizonMetaV2 {
  phase_mode: PhaseMode
  hold_target_ms: number
  alpha_half_life_ms: number
  max_signal_age_ms: number
  risk_horizon_bucket: HorizonBucket
  profile_source: string
  profile_conf: number             // 0..1
  reason_code: string              // HZ_* constant
  reason_details?: Record<string, unknown>
}

/** ATR profile snapshot at signal decision time. */
export interface AtrProfileMetaV2 {
  mode: AtrMode
  atr_value: number
  atr_tf_ms: number
  atr_window_n: number
  atr_age_ms: number
  atr_source: AtrSource
  atr_regime_value: number
  atr_trail_value: number
  atr_regime_tf_ms: number
  atr_trail_tf_ms: number
  atr_pct: number
  vol_ratio_fast_slow: number
  vol_ratio_z: number
}

/**
 * Full signal meta payload (contract_ver=2).
 *
 * Existing fields (sl_mode, sl_atr_mult, regime, dq_flags, ml_confirm_p)
 * are preserved as-is for backward compatibility.
 * New fields (horizon, atr_profile) are optional for graceful degradation.
 */
export interface SignalMetaV2 {
  contract_ver: 2                  // Always 2 for new payloads
  sl_mode: string
  sl_atr_mult: number
  regime?: string
  delta_z_threshold?: number
  dq_flags?: string[]
  ml_confirm_p?: number
  // Phase 0 new fields (optional for backward compat)
  horizon?: HorizonMetaV2
  atr_profile?: AtrProfileMetaV2
  // Fingerprinting / replay
  payload_sha1?: string
  payload_bytes?: number
  trace_meta_key?: string
}

/** Legacy meta (contract_ver < 2 or absent). */
export interface SignalMetaV1 {
  sl_mode?: string
  sl_atr_mult?: number
  regime?: string
  dq_flags?: string[]
  ml_confirm_p?: number
  [key: string]: unknown
}

/** Union type allowing both old and new consumer code. */
export type SignalMeta = SignalMetaV1 | SignalMetaV2

// ─── Type guards ──────────────────────────────────────────────────────────────

/** Returns true if meta is contract_ver=2 (Phase 0+). */
export function isSignalMetaV2(meta: unknown): meta is SignalMetaV2 {
  return (
    typeof meta === 'object' &&
    meta !== null &&
    (meta as Record<string, unknown>)['contract_ver'] === 2
  )
}

/** Returns true if meta has horizon profile. */
export function hasHorizonProfile(meta: unknown): meta is SignalMetaV2 & { horizon: HorizonMetaV2 } {
  return isSignalMetaV2(meta) && typeof (meta as SignalMetaV2).horizon === 'object'
}

/** Returns true if meta has ATR profile. */
export function hasAtrProfile(meta: unknown): meta is SignalMetaV2 & { atr_profile: AtrProfileMetaV2 } {
  return isSignalMetaV2(meta) && typeof (meta as SignalMetaV2).atr_profile === 'object'
}

// ─── Safe accessor helpers ────────────────────────────────────────────────────

/** Safe read sl_mode from any meta version. */
export function getSlMode(meta: SignalMeta | null | undefined): string {
  return (meta as SignalMetaV1 | undefined)?.sl_mode ?? 'ATR'
}

/** Safe read sl_atr_mult from any meta version. */
export function getSlAtrMult(meta: SignalMeta | null | undefined): number {
  return (meta as SignalMetaV1 | undefined)?.sl_atr_mult ?? 1.5
}

/** Safe read risk_horizon_bucket (returns 'unknown' for legacy meta). */
export function getRiskHorizonBucket(meta: SignalMeta | null | undefined): HorizonBucket {
  if (hasHorizonProfile(meta)) {
    return meta.horizon.risk_horizon_bucket
  }
  return 'unknown'
}

/** Safe read atr_value from any meta version. */
export function getAtrValue(meta: SignalMeta | null | undefined, fallback = 0): number {
  if (hasAtrProfile(meta)) {
    return meta.atr_profile.atr_value
  }
  return fallback
}

// ─── Phase 0 bootstrap factory (for testing / mock data) ─────────────────────

/** Create a minimal Phase 0 SignalMetaV2 for tests / mock responses. */
export function makePhase0SignalMeta(overrides?: Partial<SignalMetaV2>): SignalMetaV2 {
  const horizon: HorizonMetaV2 = {
    phase_mode: 'off',
    hold_target_ms: 0,
    alpha_half_life_ms: 0,
    max_signal_age_ms: 0,
    risk_horizon_bucket: 'unknown',
    profile_source: 'static_bootstrap',
    profile_conf: 0,
    reason_code: HorizonReasonCode.HZ_STATIC_BOOTSTRAP,
    reason_details: {},
  }
  const atr_profile: AtrProfileMetaV2 = {
    mode: 'legacy',
    atr_value: 0,
    atr_tf_ms: 60_000,
    atr_window_n: 14,
    atr_age_ms: 0,
    atr_source: 'legacy',
    atr_regime_value: 0,
    atr_trail_value: 0,
    atr_regime_tf_ms: 60_000,
    atr_trail_tf_ms: 60_000,
    atr_pct: 0,
    vol_ratio_fast_slow: 1,
    vol_ratio_z: 0,
  }
  return {
    contract_ver: 2,
    sl_mode: 'ATR',
    sl_atr_mult: 1.5,
    horizon,
    atr_profile,
    ...overrides,
  }
}
