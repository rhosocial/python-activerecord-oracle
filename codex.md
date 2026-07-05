# Codex Project Instructions

## Scope
- This directory is an independent GitHub repository.
- It is released to PyPI independently of other repos in this workspace.

## Primary Guidelines (Required)
- Use the main project guidelines located at `../python-activerecord/.gemini/`:
  - `../python-activerecord/.gemini/architecture.md` (project architecture)
  - `../python-activerecord/.gemini/backend_development.md` (backend package guidance)
  - `../python-activerecord/.gemini/code_style.md`
  - `../python-activerecord/.gemini/testing.md`
  - `../python-activerecord/.gemini/version_control.md`
- These references are relative paths. Developers must keep this repo and `python-activerecord` as sibling directories in the same parent folder for the links to resolve.
- If multiple files apply, prioritize the more specific guideline over general ones.

## Collaboration Rules
- Keep changes confined to this repo unless explicitly requested otherwise.
- Follow local repository docs (e.g., `README`, `CONTRIBUTING`) for build/test workflows.
- Do not assume cross-repo dependencies are installed or available.

## Quality
- Prefer minimal, targeted edits.
- If tests are requested, run them from this repo root.
