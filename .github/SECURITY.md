# Security Policy — workflow-stream-gateway

## Supported Versions

| Version | Supported |
| --------- | ----------- |
| `main` (latest) | Active |
| Older tags | Not supported |

Only the latest commit on `main` and the most recent tagged release receive security fixes. Patch the version you run by updating to the latest tag.

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report vulnerabilities privately via [GitHub Security Advisories](https://github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/security/advisories/new).

Include:

- A clear description of the vulnerability
- Steps to reproduce (minimal reproduction preferred)
- Impact assessment (what data or systems are affected)
- Any suggested fix or workaround

**Response SLAs:**

| Severity | Acknowledgement | Patch Target |
| ---------- | ---------------- | -------------- |
| Critical (CVSS ≥ 9.0) | 24 h | 72 h |
| High (CVSS 7.0–8.9) | 48 h | 7 days |
| Medium (CVSS 4.0–6.9) | 5 business days | 30 days |
| Low (CVSS < 4.0) | 10 business days | Next release |

## Scope

**In scope:**

- SQL injection, authentication bypass, tenant isolation leaks (RLS bypass)
- Unauthorized event publishing or consumption
- BPMN XML parser vulnerabilities (XXE, billion laughs)
- Secrets exposure in logs or error responses
- Privilege escalation via role header manipulation

**Out of scope:**

- Vulnerabilities in upstream dependencies not yet patched by their maintainers
- Rate-limiting bypass (handled at the Envoy gateway layer)
- Social engineering or phishing

## Dependency Vulnerabilities

Dependencies are scanned by `govulncheck` on every CI run and by Dependabot weekly. If you find a vulnerable transitive dependency, please report it via the advisory process above rather than opening a public issue.

## Trust Model

This service sits behind the Envoy API Gateway. It trusts the following gateway-injected headers as pre-authenticated:

- `x-tenant-id`, `x-user-id`, `x-tenant-roles`, `x-plan`, `x-feature-flags`

**Any bypass of Envoy that allows direct service access is a critical security vulnerability.**
