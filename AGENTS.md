# Data Layer Agent Rules

This tracked file carries the workspace baseline into every clone of this
repository. For Data Layer work, the mandatory main journal is
`DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`; detailed design is governed by
`upgrade/quant-data-layer-fund-grade-upgrade-architecture.md` and applicable
provider guides. Host-level `/home/bobby/AGENTS.md`, when present, may add
stricter workspace rules but may not weaken this file.

## Scope And Sources Of Truth

1. Before acting, read the nearest `AGENTS.md`, the project's main implementation plan, the approved detailed guide, and the relevant code/config. Do not rely on chat memory alone.
2. When the user asks to discuss, evaluate, explain, or plan only, do not edit files, restart services, mutate data, or run destructive commands.
3. Implement only the approved scope. Report newly discovered out-of-scope bugs and proposed fixes before changing them unless the user explicitly authorized fixing all discovered issues.
4. Preserve the declared domain source of truth. For alpha migrations, backtest/approved research logic and parameters are authoritative unless the user says otherwise. Do not silently simplify business logic.

## Mandatory Plan Journal

5. Every project change must be recorded in that project's main plan markdown. The plan update is part of the implementation transaction, not optional documentation after the fact.
6. Before code changes, record or confirm the phase/task status, goal, guide links, approved scope, invariants, test gates, rollback, and decision boundary in the main plan.
7. During implementation, update the plan incrementally after each coherent tested slice with exact work completed, commands/tests actually run, results, cleanup evidence, decisions, and remaining debt.
8. Never claim a task or phase is complete until the main plan and any governing detailed guide agree with the code and evidence. A final response must state the recorded status and link/path.
9. Technical debt means a real external, cost, infrastructure, licensing, business-semantics, or approval gate. Fix in-scope defects before closure; do not relabel unfinished work or failed tests as debt.
10. If a project has no identified main plan, identify or create one before substantial implementation. Do not scatter progress across ad hoc markdown files without linking them from the main plan.

## Runtime And Data Safety

11. Protect running production/V1 consumers by default. Use isolated names, ports, networks, schemas, Redis prefixes, volumes, topics, consumer groups, credentials, and evidence paths for development and tests.
12. Do not restart, recreate, cut over, prune, flush, truncate, force-close, delete volumes/data, or change authority unless the user explicitly approves the exact blast radius and rollback.
13. Production/shadow market data must come from approved real providers or replay of durably captured provider bytes. Synthetic/generated data is test-only and must carry test provenance.
14. Never commit secrets, raw credentials, private keys, unbounded logs, caches, runtime state, or generated data. Evidence stores identifiers, hashes, bounded metrics, and provenance only.
15. Clean disposable test resources and scoped smoke rows after verification. Never clean shared state broadly when an exact alpha/account/test namespace is available.

## Verification And Evidence

16. Test correctness before performance: identity, units, decimals, timestamps/timezones, ordering, bar closure, sequence, source authority, state transitions, risk/order semantics, and business-domain parity.
17. Use applicable unit, contract/golden, parity/oracle, migration-idempotency, integration, failure/reconnect, recovery/rollback, compatibility, security, resource/capacity, and bounded real-provider tests.
18. A health endpoint or process-up state alone is not acceptance. Report exact cases, pass/fail/skip counts, untested boundaries, production mutations, and cleanup results.
19. Never present local/same-host rehearsal as production, HA, independent failure-domain, broker-authoritative, sandbox/live, or real-provider evidence. Missing evidence fails closed and is documented honestly.
20. Read large logs and databases intelligently: filter relevant time ranges and warning/error/event IDs first, then query exact related rows. Avoid unbounded output and token-heavy tool use.

## Git And Change Discipline

21. Inspect branch/worktree before editing. Preserve user/unrelated changes and never revert them without explicit instruction.
22. Use feature branches from `dev` when the repository workflow requires it. Feature branches merge to `dev`; `main` is release-only. Never merge or push unless explicitly requested.
23. Commit each coherent, tested implementation slice with a clear message. Do not create excessive tiny commits or one giant untraceable commit.
24. Commits must use the user's configured identity (`BobbyAxerol <vugioan11022002@gmail.com>`), never a system/root identity. Verify identity before commit.
25. Before commit: run `git diff --check`, inspect staged scope, confirm tests/evidence and plan updates, and exclude unrelated files. After commit: report commit SHA, branch, worktree state, and whether anything was pushed/merged.

## Engineering And Operations

26. Prefer existing shared runtime, SDK, contracts, wrappers, parsers, and domain abstractions. Copy unchanged code mechanically when appropriate; avoid duplicated strategy-specific infrastructure.
27. Keep public V1/stable endpoints and schemas backward-compatible unless an approved versioned migration says otherwise. Internal implementation language and transport must stay behind contracts.
28. Design provider-neutral, venue-capability-driven, scalable boundaries. Do not hardcode one venue/mode where the domain needs extension to Binance, OKX, DNSE/VN, Deribit, or future brokers.
29. Optimize only after correctness. Measure CPU, memory, disk, queue/lag, I/O and latency; require no unexplained loss, duplicate, gap, or state mismatch before promotion.
30. Keep logs structured, host-visible where required, bounded and aligned to strategy/feed intervals. Avoid tick-level INFO spam unless actively diagnosing a new feed.
31. Use the smallest useful set of tools and agents. Prefer `rg`, targeted reads, scoped tests, reusable commands, and compact evidence over broad scans or repeated full-output calls.
32. Use `apply_patch` for manual edits when available. If the sandbox helper is broken, use an exact-match scripted replacement and immediately verify the diff.

## Reporting

33. Working updates explain what is being checked or changed and why. Final reports state: implemented scope, domain behavior, tests/evidence, runtime impact, cleanup, remaining decision gates, commit/branch, and the next permitted step.
34. Do not hide blockers or overstate readiness. Distinguish `implemented`, `tested locally`, `shadow-certified`, `production-ready`, and `production-authoritative` explicitly.
