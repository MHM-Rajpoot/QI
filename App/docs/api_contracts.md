# API Contracts

## Response Envelope

All JSON API responses use a versioned envelope:

```json
{
  "status": "success",
  "version": {
    "api": "2026.03.0",
    "contract": {
      "id": "programme_plans.page",
      "version": "v2"
    }
  },
  "data": {},
  "meta": {},
  "message": "Optional summary"
}
```

Error responses follow the same version model:

```json
{
  "status": "error",
  "version": {
    "api": "2026.03.0",
    "contract": {
      "id": "programme_plans.page",
      "version": "v2"
    }
  },
  "error": {
    "message": "page_size must be less than or equal to 500",
    "field": "page_size"
  }
}
```

## Versioning Rules

- `version.api` is the global response-output version.
- `version.contract.id` identifies the logical endpoint contract.
- `version.contract.version` changes when that endpoint output shape changes.
- The same values are also sent in response headers:
  - `X-API-Version`
  - `X-Contract-Id`
  - `X-Contract-Version`

## Discovery

- Runtime registry: `GET /api/contracts`
- Source registry: `contracts/api.py`

## Current Notable Contracts

- `programme_plans.filters@v2`: explorer metadata with quick filters, filter definitions, dataset info, and page-size options.
- `programme_plans.page@v2`: server-side paginated Programme Plans rows with pagination metadata.
- `jobs.status@v1`: queued/running/completed/failed admin job lifecycle.
