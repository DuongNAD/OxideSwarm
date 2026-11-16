# 0070: Do not auto-load configuration files from the working directory

Status: Implemented
Created: 2026-11-15    Updated: 2026-11-15

## Summary

Stop `load_config_file` (`crates/cli/src/config.rs`) from auto-loading `./rusty-grid.toml` or
`./rusty-grid.json` from the current working directory for any command (`SEC-10`, owner decision D8
default). Every command now follows the rule previously used only by `oxideswarm mcp`: auto-load
`--config <PATH>`, `RUSTY_GRID_CONFIG`, or `~/.oxideswarm/rusty-grid.toml` (or
`$OXIDESWARM_HOME/rusty-grid.toml`), and print a one-line notice to `stderr` when a working-directory
`./rusty-grid.toml` or `./rusty-grid.json` is skipped.

## Context

Before this change, `load_config_file` in `crates/cli/src/config.rs` called
`load_config_file_with_options(explicit_path, true)`, which checked `./rusty-grid.toml` and
`./rusty-grid.json` in the current working directory ahead of `~/.oxideswarm/rusty-grid.toml` with
no notice. Only `oxideswarm mcp` called `load_mcp_config_file` (`allow_working_dir = false`,
`crates/cli/src/cli_app.rs`). Every other CLI subcommand (`worker`, `submit`, `status`, `workers`,
`workspaces`, `mapreduce`, `doctor`, `master`, `secret`, `join`, `invite`, `usb`, `members`) and
`ox-mode` (`crates/cli/src/mode_cli.rs`) auto-loaded a working-directory file when present.

Because `ConfigFile` (`crates/cli/src/config.rs`) can set `master`, `p2p_ticket`, `relay_url`,
`secret`, `secret_file`, `master_key`, and `insecure`, running `oxideswarm submit --workspace ...`
or `oxideswarm worker` inside an untrusted repository checkout that contained a `rusty-grid.toml`
or `rusty-grid.json` could silently redirect the client or worker to another master and opt into
`insecure = true`. In addition, `warn_config_ticket` in `switch_role.sh` checked `./rusty-grid.toml`
and `./rusty-grid.json` in the working directory before `~/.oxideswarm/rusty-grid.toml`.

## Goals

- **G1** Never auto-load `./rusty-grid.toml` or `./rusty-grid.json` from the current working
  directory unless `--config <PATH>` or `RUSTY_GRID_CONFIG` explicitly names it (or the working
  directory is `~/.oxideswarm` / `$OXIDESWARM_HOME` and `./rusty-grid.toml` is the same file as the
  default user config).
- **G2** When a working-directory `./rusty-grid.toml` or `./rusty-grid.json` is skipped, print one
  line to `stderr` only (`Skipping config file ./<candidate> in the working directory (pass --config <PATH> or RUSTY_GRID_CONFIG to load it).`) so `--json` output and `oxideswarm mcp` stdio stay clean.
- **G3** Simplify `crates/cli/src/config.rs` and `crates/cli/src/cli_app.rs` by removing the
  `allow_working_dir` split and `load_mcp_config_file`.

## Non-goals

- Changing wire protocol frames or the `ConfigFile` schema.
- Changing `~/.oxideswarm/rusty-grid.toml` (or `$OXIDESWARM_HOME/rusty-grid.toml`) as the default
  per-user configuration path.

## Design

### D1 Unified configuration loader (`crates/cli/src/config.rs`)

`load_config_file(explicit_path: Option<&Path>)` delegates path selection to
`resolve_config_file_selection(explicit_path, env_config, working_dir, oxideswarm_dir)`:

1. If `explicit_path` (`--config <PATH>`) is `Some`, load that path and emit no skip notice.
2. Else if `RUSTY_GRID_CONFIG` is set in the environment, load that path and emit no skip notice.
3. Else select `<oxideswarm_dir>/rusty-grid.toml` (`~/.oxideswarm/rusty-grid.toml` or
   `$OXIDESWARM_HOME/rusty-grid.toml`) if it exists. If `./rusty-grid.toml` or `./rusty-grid.json`
   exists in `working_dir` and is not `is_same_file` as the selected `<oxideswarm_dir>/rusty-grid.toml`,
   print the one-line skip notice to `stderr`.

`run_cli` in `crates/cli/src/cli_app.rs` calls `load_config_file(cli.config.as_deref())` for every
subcommand, including `mcp`.

### D2 Launcher script alignment (`switch_role.sh`)

`warn_config_ticket` in `switch_role.sh` inspects `RUSTY_GRID_CONFIG` and then
`${STATE_DIR}/rusty-grid.toml` (`${OXIDESWARM_HOME:-${HOME}/.oxideswarm}/rusty-grid.toml`), matching
the CLI's auto-load order without checking `./rusty-grid.toml` or `./rusty-grid.json` in `$PWD`.

## Compatibility

No wire-format change. Users, scripts, or service definitions that kept a `rusty-grid.toml` or
`rusty-grid.json` in the working directory and relied on implicit auto-loading must now pass
`--config ./rusty-grid.toml` (or set `RUSTY_GRID_CONFIG`) or move the file to
`~/.oxideswarm/rusty-grid.toml`. All repository launchers and service installers (`connect_remote.sh`,
`packaging/android/termux/start_worker.sh`, `packaging/macos/install_mac_daemon.sh`,
`packaging/windows/install_windows_service.ps1`, `packaging/usb/install.*`, `test_integration.sh`,
`run_chaos_test.sh`, `run_cluster_benchmark.sh`) already pass `--config` explicitly or use
`~/.oxideswarm/rusty-grid.toml` / `OXIDESWARM_HOME`.

## Security

Cloning or entering an untrusted repository and running `oxideswarm submit`, `oxideswarm worker`,
`oxideswarm status`, `oxideswarm doctor`, or `ox-mode` can no longer silently override `master`,
`p2p_ticket`, `relay_url`, `secret`, `secret_file`, `master_key`, or `insecure` via a repository-supplied
`./rusty-grid.toml` or `./rusty-grid.json`.

## Test plan

- Unit test `config::tests::test_resolve_config_file_selection_skips_working_directory_files` in
  `crates/cli/src/config.rs` covering working-directory `.toml` and `.json` skipping, `~/.oxideswarm`
  fallback, same-file suppression when `working_dir == oxideswarm_dir`, and explicit `--config` /
  `RUSTY_GRID_CONFIG`.
- Integration test `test_working_directory_config_is_skipped_by_submit_status_and_worker` in
  `tests/cli_secret_e2e.rs` running `submit`, `status` (including `--json`), and `worker` in a
  temporary working directory containing `./rusty-grid.toml` and `./rusty-grid.json` with a temporary
  `HOME`.
- Regression suites: `mcp_server_e2e`, `cli_join_e2e`, `python3 tests/test_launcher_scripts.py`, and
  `python3 tests/test_m1_challenger_hygiene.py`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| SEC-10 | Skip working-directory `./rusty-grid.{toml,json}` across all CLI commands with a `stderr` notice | `crates/cli/src/config.rs`, `crates/cli/src/cli_app.rs`, `switch_role.sh`, `tests/cli_secret_e2e.rs`, `README.md`, `CLAUDE.md`, `CHANGELOG.md`, `docs/README.md`, `docs/design/0070-no-working-directory-config.md` | Unit and E2E tests (`cli_secret_e2e`, `mcp_server_e2e`, `cli_join_e2e`, launcher/hygiene scripts) pass | Implemented |

## Status log

- 2026-11-15: Implemented `SEC-10` across `crates/cli`, `switch_role.sh`, `tests/cli_secret_e2e.rs`,
  and documentation.
