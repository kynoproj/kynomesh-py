# Release Process

This project publishes to PyPI automatically when a version tag is pushed.

## Versioning

The package version is **not** hardcoded in `pyproject.toml`. It is read
dynamically from `__version__` in
[`src/kynomesh/__init__.py`](src/kynomesh/__init__.py) via Hatch's
`tool.hatch.version` setting. The value committed there is only a local/dev
fallback — the publish workflow overwrites it to match the release tag before
building.

## Cutting a release

1. Make sure `main` is green (lint + tests passing).
2. Create and push a tag in the form `vMAJOR.MINOR.PATCH`, for example:

   ```bash
   git tag v1.2.3
   git push origin v1.2.3
   ```

3. Pushing the tag triggers the [`Publish`](.github/workflows/publish.yml)
   GitHub Action, which:
   - Strips the leading `v` from the tag (`v1.2.3` -> `1.2.3`).
   - Writes that version into `__version__` in `src/kynomesh/__init__.py`.
   - Builds the sdist and wheel with `uv build`.
   - Publishes both to PyPI with `uv publish`.

No manual version bump or commit is required — the tag is the single source of
truth for the published version.

## Tag format

Tags must match `v*.*.*` (e.g. `v0.2.0`, `v1.0.0`). Tags that don't match this
pattern will not trigger a publish.
