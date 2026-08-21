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
	TenantID     string    `json:"tenant_id"`
	UserID       string    `json:"user_id"`
	TenantRoles  []string  `json:"tenant_roles"`
	Departments  []string  `json:"departments"`
	Plan         string    `json:"plan"`
	FeatureFlags []string  `json:"feature_flags"`
	IssuedAt     time.Time `json:"issued_at"`
}
