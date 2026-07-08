# Contributing

Thanks for helping improve `data_layer`. This service is used as a market-data gateway by execution and alpha services, so changes should keep API contracts stable and observable.

## Branch Flow

- Do not commit directly to `main`.
- Work on `dev` or a feature branch from `dev`.
- Open pull requests into `dev` first.
- Merge `dev` into `main` only through a release pull request after tests and smoke checks pass.
- Keep commits focused and use messages that name the subsystem and behavior changed.

Recommended flow:

```bash
git checkout dev
git pull origin dev
git checkout -b feature/binance-derivatives-contract
```

## Local Checks

This server is Docker-first. Prefer container tests over installing Python packages on the host:

```bash
docker compose run --rm test_runner python -m unittest discover -s tests
```

For changed Python files, a quick compile check is useful:

```bash
python3 -m py_compile app/path/to_file.py
```

## Pre-Commit

Install hooks in a development environment:

```bash
pre-commit install
pre-commit run --all-files
```

The hook set checks whitespace, YAML, merge conflicts, large files, and Ruff linting.

## API Contract Rules

- Keep existing response shapes stable unless a migration plan is documented.
- Add new endpoints instead of mutating old endpoint contracts when downstream services already depend on them.
- Put provider-specific raw fields under explicit payload sections and include metadata such as provider, market, params, cached, and stored.
- Do not let alpha containers call external providers directly. Add or extend `data_layer` wrappers instead.
- Do not store ephemeral crypto derivatives metrics unless a design note explicitly approves it.

## Documentation

Update these files when changing public behavior:

- `README.md` for project-level capabilities and quickstart.
- `DATA_LAYER_SERVICE_ACCESS_GUIDE.md` for downstream service contracts.
- Tests under `tests/` for endpoint and SDK behavior.

## Pull Request Checklist

- [ ] I did not commit directly to `main`.
- [ ] I updated docs for public API or operational changes.
- [ ] I added/updated tests for the changed contract.
- [ ] Docker unit tests pass or the reason is documented.
- [ ] No secrets, credentials, generated logs, parquet data, or local caches are included.
