// Package domain holds the ticket/connection/event-frame types this
// service operates on — no behavior, no dependencies on usecase or
// adapter (LLD §10.1).
package domain

import "time"

// Ticket is the snapshot of a caller's gateway-injected identity headers,
// taken at mint time (LLD §4's ssegw:ticket:<token> key). It is the *only*
// source of connection identity GET /api/v1/stream ever trusts (LLD §9.1) —
// never a client-supplied header or query value.
type Ticket struct {
	TenantID     string           `json:"tenant_id"`
	UserID       string           `json:"user_id"`
	TenantRoles  []string         `json:"tenant_roles"`
	Departments  []DepartmentRole `json:"departments"`
	Plan         string           `json:"plan"`
	FeatureFlags []string         `json:"feature_flags"`
	IssuedAt     time.Time        `json:"issued_at"`
}

// DepartmentRole is one parsed entry from x-departments' "<department_id>:
// <role>" pairs — mirrors execution_service's own port.DepartmentRole,
// parsed independently here (no shared package exists between the two
// repos for this; header parsing is each service's own local concern
// platform-wide). Role is carried for future finer-grained gating but not
// consumed by any check today — department-scoped broadcast eligibility
// (LLD §6) only cares about DepartmentID identity.
type DepartmentRole struct {
	DepartmentID string `json:"department_id"`
	Role         string `json:"role"`
}
