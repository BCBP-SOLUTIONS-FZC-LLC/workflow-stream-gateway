## Pull Request Checklist

Thank you for your contribution! Please complete the checklist below before requesting review.

### Description
<!-- A clear, concise summary of what this PR changes and why. -->

### Type of change

- [ ] Bug fix (non-breaking)
- [ ] New feature (non-breaking)
- [ ] Breaking change (requires version bump + migration guide)
- [ ] Refactor / cleanup
- [ ] Documentation / config only

---

### Code checklist

- [ ] `make lint` passes with no new warnings
- [ ] `make test` passes (unit tests with race detector)
- [ ] `make test-integration` passes (requires Docker)
- [ ] `make cover-check` passes the coverage gate
- [ ] New code follows the import rules (`domain ← usecase ← adapter`, `make arch-lint`)

### Events / messaging

- [ ] No event changes in this PR  
- OR:
- [ ] This service publishes no events; a new outbound event designs its contract and names a consumer first
- [ ] Inbound event handling (`POST /api/v1/internal/events`) tolerates redelivery
- [ ] `api/asyncapi.yaml` lists every event type the inbound handler accepts

### Tests

- [ ] Unit tests added or updated for changed behaviour
- [ ] Integration tests added for any new Valkey adapter method (`make test-integration`)
- [ ] No `t.Skip` placeholders removed without implementing the test body

### Documentation

- [ ] README updated if setup steps or environment variables changed
- [ ] `.claude/CLAUDE.md` updated if a critical guardrail changed
- [ ] ARCHITECTURE.md, the LLD and `docs/swagger/openapi.yaml` updated if behaviour or routes changed
- [ ] CHANGELOG.md entry added under `## [Unreleased]`

### Security

- [ ] No secrets, tokens, or DSNs in committed code
- [ ] Stream identity comes only from the ticket, never from a client-supplied value

---

### Screenshots / logs (if applicable)
<!-- Paste relevant log output, curl responses, or screenshots here. -->
