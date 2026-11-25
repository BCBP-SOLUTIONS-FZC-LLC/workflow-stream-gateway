# Ticket mint from gateway identity headers, and the stream's refusals.

POST {{host}}/api/v1/stream/tickets
x-tenant-id: {{tenant}}
x-user-id: {{user}}
x-tenant-roles: member
x-departments: {{department}}:approver
HTTP 201
[Asserts]
jsonpath "$.ticket" matches "^[A-Za-z0-9_-]{43}$"
jsonpath "$.expires_in" == 60

# Ids form the Valkey keys, so only lower-case UUIDs are minted.
POST {{host}}/api/v1/stream/tickets
x-tenant-id: {{tenant}}
x-user-id: user-1
x-tenant-roles: member
HTTP 401
[Asserts]
jsonpath "$.code" == "unauthorized"
jsonpath "$.detail" == "invalid gateway identity headers"

POST {{host}}/api/v1/stream/tickets
x-tenant-id: {{tenant}}:other
x-user-id: {{user}}
x-tenant-roles: member
HTTP 401

POST {{host}}/api/v1/stream/tickets
x-tenant-id: {{tenant}}
x-user-id: {{user}}
x-tenant-roles: member
x-departments: dept|x:approver
HTTP 401

# No gateway identity at all is refused before the handler runs.
POST {{host}}/api/v1/stream/tickets
HTTP 401

GET {{host}}/api/v1/stream
HTTP 401
[Asserts]
jsonpath "$.code" == "unauthorized"
jsonpath "$.detail" == "missing ticket query parameter"

GET {{host}}/api/v1/stream?ticket=not-a-ticket-{{run}}
HTTP 401
[Asserts]
jsonpath "$.code" == "unauthorized"
