# Governance

This document describes how OxideSwarm is governed today and how decisions, contributions, and maintainership work.

## Roles today

- **Owner (`DuongNAD`):** Sets project direction, decides scope and open design questions (recorded in [docs/ROADMAP.md](docs/ROADMAP.md) and [docs/BACKLOG.md](docs/BACKLOG.md)), reviews pull requests, and holds merge rights on `master` and release tag-push rights.
- **Coding agents:** Write, test, and review code and documentation against the design records and repository checks ([CONTRIBUTING.md](CONTRIBUTING.md)).
- **Contributors:** Anyone opening issues, reporting bugs, running studies or benchmarks, or submitting pull requests.

## Decision-making

- Behavioural changes, CLI flags, environment variables, wire format changes, and major architecture plans are written as design records under [docs/design/](docs/design/) before or alongside code changes.
- Policy and scope questions for the owner are tracked in the decisions tables of [docs/ROADMAP.md](docs/ROADMAP.md) and [docs/BACKLOG.md](docs/BACKLOG.md).
- A pull request merges only when all required CI checks pass, the change has been reviewed, and the owner approves the merge.

## Becoming a maintainer

Today the project has one human owner and coding agents. A human contributor may become a maintainer through sustained, high-quality code or review contributions across releases, by invitation from the owner.

Maintainer status grants:

- Review and merge approval on pull requests within agreed areas.
- Permission to prepare and publish releases following [docs/guides/cut-a-release.md](docs/guides/cut-a-release.md).

## Future stewardship

Before any open public compute network launches, governance is planned to transition to a neutral steward alongside an open protocol specification ([docs/ROADMAP.md](docs/ROADMAP.md) decision Q17, [design 0054](docs/design/0054-open-compute-network.md), [design 0058](docs/design/0058-lasting-value.md)).

## Related documents

- [CONTRIBUTING.md](CONTRIBUTING.md)
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- [docs/SECURITY.md](docs/SECURITY.md)
