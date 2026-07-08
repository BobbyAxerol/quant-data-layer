# Security Policy

`data_layer` handles market-data connectivity and service-to-service data distribution. It should never contain trading credentials, broker secrets, personal API keys, or production account identifiers in source control.

## Reporting a Vulnerability

Please report security issues privately to the maintainers. Do not open a public issue with exploit details, credentials, or production logs.

Include:

- affected component or endpoint;
- reproduction steps;
- expected impact;
- relevant logs with secrets removed;
- suggested mitigation, if known.

## Supported Security Practices

- Keep `.env` local and out of git.
- Use Docker networks for internal services.
- Do not expose Redis or provider credentials publicly.
- Prefer API wrappers in `data_layer` over direct provider calls from alpha containers.
- Redact API keys, tokens, account IDs, and investor identifiers before sharing logs.

## Dependency Updates

Dependency and Docker image updates should be tested in `dev` before merge to `main`.
