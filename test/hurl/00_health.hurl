# Liveness, readiness and unmatched routes.

GET {{host}}/healthz
HTTP 200

GET {{host}}/readyz
HTTP 200

GET {{host}}/api/v1/nowhere
HTTP 404
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "not-found"
jsonpath "$.status" == 404
jsonpath "$.instance" == "/api/v1/nowhere"

# An encoded slash reaches no handler, and the problem names the path as sent.
POST {{host}}/api/v1/internal%2Fevents
x-user-id: iam-system
x-tenant-roles: iam-system
{}
HTTP 404
[Asserts]
jsonpath "$.detail" == "no route matches POST /api/v1/internal%2Fevents"
jsonpath "$.instance" == "/api/v1/internal%2Fevents"
