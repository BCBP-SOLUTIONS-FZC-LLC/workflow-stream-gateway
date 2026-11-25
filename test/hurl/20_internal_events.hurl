# Event ingest as the system caller (the caller identity check is off locally).

POST {{host}}/api/v1/internal/events
x-user-id: iam-system
x-tenant-roles: iam-system
{
  "id": "0190c6a2-7b3e-7f00-9a1b-2c3d4e5f6a70",
  "type": "WorkflowTaskCreated",
  "source": "workflow-execution-svc",
  "specversion": "1",
  "tenant_id": "{{tenant}}",
  "time": "2026-11-25T10:00:00Z",
  "data": {"recipients": [{"kind": "user", "id": "{{user}}"}, {"kind": "department", "id": "{{department}}"}]}
}
HTTP 200

# A pair of source and type the routing file does not list is acknowledged and ignored.
POST {{host}}/api/v1/internal/events
x-user-id: iam-system
x-tenant-roles: iam-system
{"id": "0190c6a2-7b3e-7f00-9a1b-2c3d4e5f6a71", "type": "WorkflowInstanceStarted", "source": "workflow-execution-svc", "specversion": "1", "tenant_id": "{{tenant}}", "time": "2026-11-25T10:00:00Z", "data": {}}
HTTP 200

# The tenant forms every stream key, so only a lower-case UUID is taken.
POST {{host}}/api/v1/internal/events
x-user-id: iam-system
x-tenant-roles: iam-system
{"id": "0190c6a2-7b3e-7f00-9a1b-2c3d4e5f6a72", "type": "WorkflowTaskCreated", "source": "workflow-execution-svc", "specversion": "1", "tenant_id": "broadcast:{{tenant}}", "time": "2026-11-25T10:00:00Z", "data": {"recipients": []}}
HTTP 400
[Asserts]
jsonpath "$.code" == "invalid-payload"
jsonpath "$.detail" contains "tenant_id must be a lower-case UUID"

POST {{host}}/api/v1/internal/events
x-user-id: iam-system
x-tenant-roles: iam-system
{"id": "0190c6a2-7b3e-7f00-9a1b-2c3d4e5f6a73", "type": "WorkflowTaskCreated", "source": "workflow-execution-svc", "specversion": "1", "tenant_id": "{{tenant}}", "time": "2026-11-25T10:00:00Z", "data": {}}
HTTP 400

POST {{host}}/api/v1/internal/events
x-user-id: iam-system
x-tenant-roles: iam-system
{"id": "0190c6a2-7b3e-7f00-9a1b-2c3d4e5f6a74", "type": "WorkflowTaskCreated", "source": "workflow-execution-svc", "specversion": "1", "tenant_id": "{{tenant}}", "time": "2026-11-25T10:00:00Z", "data": {"recipients": [{"kind": "user", "id": "a:b"}]}}
HTTP 400

POST {{host}}/api/v1/internal/events
x-user-id: iam-system
x-tenant-roles: iam-system
```
not json
```
HTTP 400

# Only the system caller identity may post.
POST {{host}}/api/v1/internal/events
x-user-id: {{user}}
x-tenant-roles: member
{}
HTTP 401
