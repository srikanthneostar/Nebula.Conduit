# Frappe HR Polling Connector Specification

## Purpose

This document defines a polling-based connector from Frappe HR to Nebula. The connector periodically reads the latest employee-related changes from Frappe HR and pushes those changes into Nebula as upserts.

The first implementation should focus on employee master data. Onboarding and other HR lifecycle objects can be added after the basic employee sync is stable.

## Integration Objective

The connector should:

- Authenticate to Frappe HR using API key and secret
- Poll Frappe HR at a fixed interval
- Fetch only new or updated employee records since the last successful checkpoint
- Transform Frappe HR records into Nebula's employee payload
- Push updates into Nebula
- Store connector state so no changes are missed across runs or restarts

## Recommended Scope For Phase 1

Poll only the `Employee` DocType in the first version.

Why:

- It gives the current employee master record
- It captures new hires after employee creation
- It captures updates like department, designation, status, company, and contact changes
- It is the simplest and most reliable polling source for an initial connector

Optional phase 2 DocTypes:

- `Employee Onboarding`
- `Employee Transfer`
- `Employee Promotion`
- `Employee Separation`

## Frappe HR Prerequisites

Before using the connector, configure the following in Frappe HR:

1. Create a dedicated integration user.
2. Generate an API key and API secret for that user.
3. Grant the user read access to the required DocTypes.
4. Confirm the Employee module is visible and records can be queried from the UI.
5. Confirm the Frappe HR base URL is reachable from the Nebula environment.

Recommended user:

- `nebula.integration@<company>`

Minimum permissions:

- Read access on `Employee`
- Read access on linked masters if Nebula later needs lookups from related DocTypes

## Authentication

Use Frappe token-based authentication:

```http
Authorization: token <api_key>:<api_secret>
```

Reference:

- https://docs.frappe.io/framework/user/en/api/rest

## Connector Configuration

The connector should support the following configuration:

```yaml
name: frappe-hr
enabled: true
baseUrl: http://frappe-host:8001
apiKey: <api_key>
apiSecret: <api_secret>
pollIntervalSeconds: 300
pageSize: 100
initialCursor: "2026-01-01 00:00:00"
overlapWindowSeconds: 120
docTypes:
  - Employee
employeeFields:
  - name
  - employee_name
  - employee_number
  - status
  - company
  - department
  - designation
  - date_of_joining
  - relieving_date
  - company_email
  - personal_email
  - cell_number
  - gender
  - date_of_birth
  - modified
  - modified_by
fullReconciliationEnabled: true
fullReconciliationCron: "0 2 * * *"
```

## Configuration Notes

- `baseUrl`: Base URL of the Frappe HR instance.
- `pollIntervalSeconds`: How often the connector polls for changes.
- `pageSize`: Page size for each list API request.
- `initialCursor`: Starting point for the first sync if no checkpoint exists.
- `overlapWindowSeconds`: Small rewind window to avoid missing records with close timestamps.
- `employeeFields`: Fields to request from Frappe HR.
- `fullReconciliationEnabled`: Optional periodic full validation job.

## Core API Endpoints

### List Employee Records

```http
GET /api/resource/Employee
```

Used for incremental polling.

Supported query parameters used by this connector:

- `fields`
- `filters`
- `order_by`
- `limit_start`
- `limit_page_length`

Reference:

- https://docs.frappe.io/framework/user/en/api/rest

### Get Single Employee Record

```http
GET /api/resource/Employee/{name}
```

Used for:

- Field discovery during setup
- Debugging individual records
- Optional re-fetch if the list response omits fields needed by Nebula

### Optional Onboarding Endpoint

```http
GET /api/resource/Employee%20Onboarding
```

This is not required for phase 1, but can be used later if Nebula needs pre-hire or onboarding workflow updates.

Reference:

- https://docs.frappe.io/hr/employee
- https://docs.frappe.io/hr/employee-onboarding

## Incremental Polling Design

Use the Frappe system field `modified` as the watermark.

The connector stores:

- `lastSuccessfulModified`
- `lastSuccessfulRecordName` as optional tie-breaker
- run metadata like last run start time, last run end time, and last error

### Polling Rule

For each run:

1. Read the current checkpoint.
2. Subtract the overlap window from the checkpoint timestamp.
3. Query Frappe HR for records where `modified` is greater than the adjusted timestamp.
4. Order by `modified asc`.
5. Paginate until all pages are consumed.
6. Deduplicate records within the run using `name + modified`.
7. Transform each record into Nebula employee format.
8. Push each record as an upsert into Nebula.
9. Save the highest processed `modified` value only after the whole run succeeds.

### Why The Overlap Window Is Needed

Timestamp-only checkpointing can miss records if:

- multiple rows share the same timestamp
- clock granularity differs between components
- a previous run completed partially before checkpoint write

Using a 2-minute overlap and deduplicating by `name + modified` makes the connector safer.

## Incremental Polling Request Example

```bash
curl --get 'http://frappe-host:8001/api/resource/Employee' \
  -H 'Authorization: token <api_key>:<api_secret>' \
  -H 'Accept: application/json' \
  --data-urlencode 'fields=["name","employee_name","employee_number","status","company","department","designation","date_of_joining","relieving_date","company_email","personal_email","cell_number","gender","date_of_birth","modified","modified_by"]' \
  --data-urlencode 'filters=[["modified",">","2026-04-16 00:00:00"]]' \
  --data-urlencode 'order_by=modified asc' \
  --data-urlencode 'limit_start=0' \
  --data-urlencode 'limit_page_length=100'
```

## Example Response Shape

```json
{
  "data": [
    {
      "name": "HR-EMP-00001",
      "employee_name": "Asha Rao",
      "employee_number": "E1001",
      "status": "Active",
      "company": "Acme India",
      "department": "Engineering",
      "designation": "Software Engineer",
      "date_of_joining": "2026-04-01",
      "relieving_date": null,
      "company_email": "asha.rao@acme.example",
      "personal_email": "asha.personal@example.com",
      "cell_number": "+919900000000",
      "gender": "Female",
      "date_of_birth": "1998-08-14",
      "modified": "2026-04-16 08:20:15.123456",
      "modified_by": "Administrator"
    }
  ]
}
```

## Recommended Nebula Mapping

The exact destination schema depends on Nebula's employee model, but this is a practical starting mapping:

| Frappe HR | Nebula |
| --- | --- |
| `name` | `sourceRecordId` |
| `employee_number` | `employeeCode` |
| `employee_name` | `fullName` |
| `status` | `employmentStatus` |
| `company` | `companyName` |
| `department` | `departmentName` |
| `designation` | `jobTitle` |
| `date_of_joining` | `joinDate` |
| `relieving_date` | `exitDate` |
| `company_email` | `workEmail` |
| `personal_email` | `personalEmail` |
| `cell_number` | `mobileNumber` |
| `gender` | `gender` |
| `date_of_birth` | `dateOfBirth` |
| `modified` | `sourceLastModified` |
| `modified_by` | `sourceModifiedBy` |

Recommended source metadata in Nebula:

- `sourceSystem = "frappe-hr"`
- `sourceEntity = "Employee"`
- `sourceRecordId = Employee.name`
- `sourceLastModified = Employee.modified`

## Upsert Contract

Nebula should treat each incoming employee event as an upsert keyed by:

- `sourceSystem`
- `sourceEntity`
- `sourceRecordId`

This avoids duplicate employee creation when the same Frappe record is seen again because of overlap-window reprocessing.

## Suggested Connector Algorithm

```text
loadCheckpoint()
pollFrom = checkpoint.modified - overlapWindow
pageStart = 0
maxModifiedSeen = checkpoint.modified

do
  response = GET /api/resource/Employee with filters on modified > pollFrom
  sort ascending by modified
  page through results

  for each employee in response.data
    uniqueKey = employee.name + "|" + employee.modified
    if alreadyProcessed(uniqueKey)
      continue

    payload = mapEmployeeToNebula(employee)
    pushToNebula(payload)
    markProcessed(uniqueKey)

    if employee.modified > maxModifiedSeen
      maxModifiedSeen = employee.modified

  pageStart += pageSize
while response.data is not empty

saveCheckpoint(maxModifiedSeen)
```

## Error Handling

The connector should treat errors differently by category.

### Retryable Errors

- Frappe HR unavailable
- connection timeout
- HTTP 429
- HTTP 500, 502, 503, 504
- temporary DNS or network failures

Recommended handling:

- Retry with exponential backoff
- Do not advance checkpoint

### Non-Retryable Errors

- HTTP 401 or 403 from Frappe HR
- invalid configuration
- invalid field mapping
- malformed request

Recommended handling:

- Fail the run
- Raise an alert
- Do not advance checkpoint

### Partial Push Failure To Nebula

If Frappe fetch succeeds but Nebula push fails:

- stop processing the run
- do not advance checkpoint
- allow next run to replay from the overlap window

This preserves at-least-once delivery semantics.

## Pagination Rules

Use:

- `limit_start`
- `limit_page_length`

Recommended defaults:

- page size `100`

Continue requesting pages until the response `data` array is empty.

## Full Reconciliation

Incremental sync is enough for normal operation, but add a periodic full reconciliation job.

Recommended frequency:

- once daily during off-hours

Purpose:

- detect records missed because of unexpected failures
- verify field mapping consistency
- compare record counts and last modified values

Full reconciliation can either:

- read all Employee records page by page, or
- read all active employees only, depending on Nebula requirements

## Handling Deletes

A polling connector against Frappe's resource API does not naturally detect hard deletes well.

Recommended approach:

- treat employee lifecycle changes as status updates instead of deletes
- use `status` and `relieving_date` for employee deactivation logic
- if hard deletes must be handled, add a reconciliation process to detect missing source records

## Data Quality And Customization Notes

Frappe HR fields can vary across environments because administrators may customize DocTypes.

Before finalizing the connector, verify:

1. exact field names in your Frappe instance
2. whether `employee_number` is populated consistently
3. which email field Nebula should consider authoritative
4. which employee statuses are used locally
5. whether custom fields need to be included

Useful validation call:

```bash
curl 'http://frappe-host:8001/api/resource/Employee/HR-EMP-00001' \
  -H 'Authorization: token <api_key>:<api_secret>' \
  -H 'Accept: application/json'
```

## Operational Recommendations

- Run the connector every 5 minutes initially
- Use a 2-minute overlap window
- Use `Employee.name` as the stable source key
- Store checkpoints durably in Nebula
- Log request URL, page number, record count, checkpoint before run, and checkpoint after run
- Mask API secrets in logs
- Add health metrics for last successful poll time and records processed

## Security Recommendations

- Use a dedicated integration user
- Grant least privilege
- Store `apiKey` and `apiSecret` in secure secret storage
- Restrict network access to the Frappe HR host
- Use HTTPS in non-local environments

## Phase 2 Extension Plan

After employee sync is stable, extend the connector to support:

- `Employee Onboarding` for pre-start workflow visibility
- `Employee Transfer` for department/company movement events
- `Employee Separation` for offboarding events

Each DocType should have its own:

- field mapping
- checkpoint
- dedupe key
- transformation contract

## References

- Frappe REST API: https://docs.frappe.io/framework/user/en/api/rest
- Frappe REST API guide: https://docs.frappe.io/framework/user/en/guides/integration/rest_api
- Frappe HR Employee docs: https://docs.frappe.io/hr/employee
- Frappe HR Employee Onboarding docs: https://docs.frappe.io/hr/employee-onboarding
- Frappe HR Employee Transfer docs: https://docs.frappe.io/hr/employee-transfer
