# cli tests

Oracle backend CLI: black-box tests against a live scenario server, offline black-box tests for the introspect and status subcommands (parser contracts and handle() branches) and Phase 5 CLI module structure/commands.

## Key files

- `test_cli_blackbox.py` — live black-box command surface
- `test_cli_introspect.py` — offline introspect subcommand
- `test_cli_status.py` — offline status subcommand
- `test_phase5_cli.py` — phase 5 CLI structure and commands
