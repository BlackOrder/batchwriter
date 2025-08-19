## v0.1.1 - 2025-08-19

### Changed
- `Push` semantics: now first performs a non-blocking enqueue attempt. If the internal queue has capacity, the item is accepted even if the writer's shutdown context has already been canceled. Cancellation (writer shutdown or caller context) is only consulted when the enqueue would otherwise block. This aligns `Push` with the existing `PushMany` behavior.

### Rationale
Previously `Push` used a single blocking select including cancellation channels, causing immediate rejection after shutdown even when buffer space remained. The new behavior allows draining producers quickly without spurious failures after initiating shutdown, matching documented intent.

### No Breaking API Change
Function signature unchanged; only timing/acceptance in shutdown window differs (more permissive).
