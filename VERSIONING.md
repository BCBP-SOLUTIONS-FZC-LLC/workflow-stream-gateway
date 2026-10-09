# Versioning and releases

`workflow-stream-gateway` is a deployable service, not a library. A release is a Git tag `vMAJOR.MINOR.PATCH` (pre-releases `-rc.N`) that triggers the `Release` workflow, which builds, scans and signs the container image. Each release is described in [CHANGELOG.md](./CHANGELOG.md).

## Semantic versioning

| Bump | When |
| --- | --- |
| MAJOR | A breaking change to a public contract: `docs/swagger/openapi.yaml` (the ticket, stream and internal ingest endpoints), the SSE frame format, or the consumed event types in `api/asyncapi.yaml` |
| MINOR | A backward-compatible capability: a new endpoint, an optional field, a new event type |
| PATCH | A backward-compatible fix, a dependency bump, or a docs-only correction |

## What is public

| In scope | Out of scope |
| --- | --- |
| Request and response bodies, status codes and error codes of the HTTP APIs | Anything under `internal/` |
| The SSE frame and resume-cursor format | Valkey key layout, which is internal |
| The consumed event types | Prometheus metric names beyond the platform standard |

## Cutting a release

1. Move `[Unreleased]` in `CHANGELOG.md` to a `## [X.Y.Z] - YYYY-MM-DD` heading and merge to `main`.
2. Tag the merged commit: `git tag vX.Y.Z && git push origin vX.Y.Z`.
3. The `Release` workflow refuses a tag that has no CHANGELOG entry.

Tags are never cut inside a pull request.
