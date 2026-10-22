---
name: Feature request
about: Propose a new feature for the workflow-stream-gateway (new endpoint, BPMN validation rule, event type, compiler behaviour, observability, etc.)
title: '[FEAT] '
labels: enhancement
assignees: ''
---

## Problem / motivation

What problem does this solve? Which user persona is affected (tenant admin, platform engineer, downstream service)?

## Proposed solution

Describe the API change, new behaviour, or new event type you'd like.

```go
// Example: new handler signature, domain type, or port interface change
```

## Design constraints

- Does this require a database migration?
- Does this introduce a new outbound event type (add to `wf.template.*` namespace)?
- Does this affect the internal compiled-plan endpoint the Execution Service reads (`api/internal-openapi.yaml`)?
- Does this change BPMN validation rules (affects existing published templates)?

## Alternatives considered

Other approaches you evaluated and why you ruled them out.

## Acceptance criteria

- [ ]
- [ ]
- [ ]

## Additional context

Links to related issues, design docs (`.design/stream_gateway.md` section), or prior art.
