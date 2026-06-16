# media-rights-check (Phase 3, §6.2/§23)

Rights + disclosure gate. `social:media:enriched` → `social:media:rights` (`media_rights.v1`).
The publish gate consumes this to keep undisclosed / unverified-rights content out of publish.

Deterministic `assess_rights`:
- AI-assisted content → always an `ai_generated` disclosure (synthetic media).
- CTA/affiliate signal (from enriched features) → `affiliate` disclosure required.
- `rights_status` from the asset's rights-ledger entry: `unverified` → needs_review,
  `infringing` → block, `owned`/`licensed` (with disclosure ready) → allow.

Engineering layer the research reports flag as non-negotiable for commercial content (FTC).
