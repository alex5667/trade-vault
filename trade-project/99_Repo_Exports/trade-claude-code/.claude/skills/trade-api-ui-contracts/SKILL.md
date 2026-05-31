---
name: trade-api-ui-contracts
description: Use this skill for NestJS, Next.js, WebSocket, DTO, validation, frontend state, UI contract, streaming updates, and backend-to-frontend integration in the trade project. Relevant for prompts about NestJS gateway, WebSocket namespaces, DTOs, Next.js hooks, signal rendering, client subscriptions, schema/versioning, and end-to-end contracts.
---

# Trade API and UI Contracts

## Goal
Keep the NestJS/Next.js layers strongly typed, versioned, observable, and stable under streaming load.

## Use this skill for
- NestJS REST/WebSocket changes
- DTO/validation/schema design
- Next.js real-time UI hooks
- Client/server contract evolution
- Streamed signal rendering and caching
- Backward-compatible payload migrations

## Contract rules
- Define DTOs explicitly; no untyped payload hand-waving.
- Version event payloads if fields may evolve.
- Distinguish transport schema from internal domain model.
- Prefer additive changes over breaking changes.
- Document nullability and optional fields.
- Validate at service boundaries.

## NestJS guidance
- Keep gateways/controllers thin.
- Push business logic into services.
- Validate inbound payloads with DTOs/pipes.
- Normalize timestamps before broadcasting.
- Emit structured event names and namespaces intentionally.

## Next.js guidance
- Keep socket hooks composable and scoped by channel/topic.
- Handle reconnect, duplicate events, and stale state.
- Prefer reducer/state-machine patterns when event volume is high.
- Surface connection status and lag in UI when relevant.

## Required deliverables
- DTOs/interfaces/types
- Gateway/controller/service changes
- Client hook/store changes
- Migration notes for existing clients
- Integration test path for server + client contract

## Tests required
- DTO validation tests
- Gateway/controller tests
- Client hook behavior tests for reconnect/dedup
- End-to-end contract tests with representative payloads

## Observability
- events_sent_total
- events_dropped_total
- ws_clients_connected
- serialization_errors_total
- client_lag or server_broadcast_latency

## Output style
Prefer exact filenames, DTO definitions, event names, sample payloads, and migration notes.\n

## Default lane
Assume **Gemini Flash + Fast mode** by default for this skill. Escalate only when the triggers below fire.

## Scope rules
- DTO / schema / WS changes are additive or bounded
- scope is limited to touched files and nearest contracts

## Escalate to premium if
- breaking or non-backward-compatible contract change is possible
- multiple transport boundaries change together
- UI/API redesign is required rather than local compatibility work

## Token discipline
- Read the smallest relevant set of files first.
- Prefer concise diffs, checklists, and test cases over long theory.
- Reuse existing repository patterns before proposing redesign.
- Avoid repository-wide scans unless an escalation trigger fires.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
