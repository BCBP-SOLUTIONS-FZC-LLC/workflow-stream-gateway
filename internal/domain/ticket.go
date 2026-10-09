package domain

import "time"

type Ticket struct {
	TenantID     string           `json:"tenant_id"`
	UserID       string           `json:"user_id"`
	TenantRoles  []string         `json:"tenant_roles"`
	Departments  []DepartmentRole `json:"departments"`
	Plan         string           `json:"plan"`
	FeatureFlags []string         `json:"feature_flags"`
	IssuedAt     time.Time        `json:"issued_at"`
}

type DepartmentRole struct {
	DepartmentID string `json:"department_id"`
	Role         string `json:"role"`
}
