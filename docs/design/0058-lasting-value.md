# 0058: Lasting value (assurance, specification, evidence, stewardship)

Status: Draft (plan; builds nothing; Q32 is decided for the first items; Q31 and Q33 to Q35 are
open)
Created: 2026-11-10    Updated: 2026-11-10

## Summary

On 2026-11-10 the owner asked for further directions that make the project as good as it can be
and worth remembering. [Design 0056](0056-frontier-program.md) plans what the grid does. This
record plans why anyone should trust it, build on it, cite it, and find it alive after its founder
has moved on. It adds no feature.

- **Assurance:** published evidence that the grid is correct and safe. Fuzzing, deterministic
  simulation, a checked model, an inventory of `unsafe`, a signed supply chain and an independent
  review.
- **Specification:** the protocol and the formats written down, with test vectors and a second
  decoder that reads them.
- **Evidence:** releases that can be cited, measurements others can use, and a case study of how
  the project is built.
- **Stewardship:** the project can be released and governed without its founder.

One rule keeps it small: every release ships at least one item of this record, and the record
takes one lane of work at most (Q31). Nothing here is built.

## Context

### Where the project stands

The state at `origin/master` `2f22c48` on 2026-11-10. The counts come from the commands in the
right column and will drift. They are not targets.

| Area | State | Checked with |
|---|---|---|
| Tests | About 2,100 test functions, integration targets registered in four crates, CI on Linux, macOS and Windows, a chaos script, golden-bytes tests of the wire layout | `grep -rnE '#\[(tokio::)?test' crates tests`, `.github/workflows/ci.yml`, `crates/core/src/wire_layout_tests.rs` |
| Fuzzing | None. No fuzz target and no property-testing dependency | No `fuzz/` directory, the `Cargo.toml` files |
| Simulation | None. `GridStream::Mock`, an in-memory duplex stream, exists and tests use it. A few tests pause time | `crates/core/src/transport.rs` |
| `unsafe` | About 240 sites in 19 files, most in `crates/worker` (process trees, isolation) and `crates/core`. No crate forbids `unsafe_code` | `grep -rnE 'unsafe (\{\|fn\|impl)' crates` |
| Dependencies | `cargo audit` on every pull request and weekly, and Dependabot. No check of licences or sources | `.github/workflows/ci.yml`, `.github/dependabot.yml` |
| Releases | Archives with `SHA256SUMS`. No signature and no build provenance | `.github/workflows/release.yml` |
| Specification | The wire format is defined by `crates/core/src/protocol.rs`, [design 0040](0040-protocol-versioning.md) and the golden-bytes tests. No document exists that a second implementer could follow | `docs/` |
| Citation and archive | No `CITATION.cff` and no DOI | Repository root |
| People | One human author since the first commit on 2026-09-19. Coding agents implement and review. `CONTRIBUTING.md` and a private route for security reports exist (REL-5). No code of conduct, no governance file, no funding file. GitHub does not detect the dual licence | `git shortlog -sn`, repository root, the GitHub repository page |

### What the research of 2026-11-10 found

Two research runs searched the web on 2026-11-10. "Basis" is used as in design 0056: **page**
means the research run reported reading the primary page, **summary** means the claim comes from
search results. A summary row is checked at its source before another document quotes it.

Assurance:

| Finding | Source | Basis | Consequence |
|---|---|---|---|
| SQLite's testing page reports test code about 590 times the size of the core, 100 % branch coverage, injected faults for memory, I/O and power loss, continuous fuzzing, and a regression test for every bug | [SQLite](https://www.sqlite.org/testing.html) | page | A small project earns trust by publishing how it is tested. LV-2, LV-7 |
| TigerBeetle runs whole clusters in one process with all I/O mocked, and injects network and storage faults | [TigerBeetle](https://tigerbeetle.com/blog/2023-07-11-we-put-a-distributed-database-in-the-browser/) (2023) | page | The model for LV-2 |
| turmoil (0.7.2, 2026-04-24) simulates hosts, sockets and a file system for code that uses its replacements for `tokio::net`. madsim replaces the runtime and needs a build flag and patched crates. shuttle and loom test thread schedules only. No page says that any of them carries QUIC | [turmoil](https://github.com/tokio-rs/turmoil), [madsim](https://github.com/madsim-rs/madsim), [shuttle](https://github.com/awslabs/shuttle) | page | The master already runs over `GridStream::Mock`, so a harness on that link is the smallest step. R11 decides |
| OSS-Fuzz accepts projects with a significant user base. ClusterFuzzLite runs fuzzing in CI on pull requests and supports Rust | [OSS-Fuzz](https://google.github.io/oss-fuzz/getting-started/accepting-new-projects/), [ClusterFuzzLite](https://github.com/google/clusterfuzzlite) | page | LV-1 runs in our own CI |
| Miri finds undefined behaviour and data races. It supports no networking and little of the platform interface | [Miri](https://github.com/rust-lang/miri) | page | LV-4 uses it for pure logic only |
| TLA+ has a public collection of specifications that CI validates, and Quint is actively developed with a simulator and a symbolic checker. Stateright had its last release on 2025-07-27 | [tlaplus/Examples](https://github.com/tlaplus/Examples), [Quint](https://github.com/informalsystems/quint), [Stateright](https://crates.io/api/v1/crates/stateright) | page | LV-3 uses TLA+ or Quint and publishes the model |
| The published analysis of the Noise `IK` pattern: the first message is open to key-compromise impersonation and has weak forward secrecy, and the later messages resist both | [Noise Explorer](https://noiseexplorer.com/patterns/IK/) | page | [Design 0051](0051-machine-identity-and-invites.md) names forward secrecy and does not state these properties of the first message. It must, before a review. LV-6 |
| The OpenSSF Best Practices badge asks at its first level for a vulnerability report process answered within 14 days, an automated test suite, static analysis and publicly reviewed cryptography. OpenSSF Scorecard checks fuzzing, signed releases, pinned dependencies and token permissions | [Criteria](https://www.bestpractices.dev/en/criteria/0), [Scorecard](https://github.com/ossf/scorecard) | page | The outside checklists of LV-5 |
| `cargo deny` checks licences, banned crates, advisories and sources. GitHub attests the build provenance of an artifact with `actions/attest`. SLSA build level 2 is signed provenance from a hosted platform | [cargo-deny](https://github.com/EmbarkStudios/cargo-deny), [SLSA levels](https://slsa.dev/spec/v1.1/levels) | page | LV-5 |
| Sovereign Tech Resilience arranges security audits of open-source projects through an application portal, and OSTIF takes requests through a form. Neither page gives a price | [Sovereign Tech Resilience](https://www.sovereign.tech/programs/bug-resilience), [OSTIF](https://ostif.org/) | page | LV-6 is an application the owner makes |

Specification, evidence and stewardship:

| Finding | Source | Basis | Consequence |
|---|---|---|---|
| A BitTorrent proposal becomes final only after a client with public code implements it. Matrix accepts a change to its specification only with a working implementation, and has a compliance test suite | [BEP 1](https://www.bittorrent.org/beps/bep_0001.html), [Matrix proposals](https://spec.matrix.org/proposals/), [Complement](https://github.com/matrix-org/complement) | page | A specification counts when something other than the first implementation passes it. LV-8 |
| GitHub reads `CITATION.cff` and shows a citation link. Zenodo, once switched on for a repository, archives each GitHub release and gives it a DOI | [Citation files](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-citation-files), [Referencing and citing](https://docs.github.com/en/repositories/archiving-a-github-repository/referencing-and-citing-content) | page | LV-10 |
| The Journal of Open Source Software takes research software with more than six months of public history and evidence of use in research. It permits generative AI with a disclosure, and requires that humans made the core design decisions and validated the output | [JOSS](https://joss.readthedocs.io/en/latest/submitting.html) | page | The first commit is from 2026-09-19. A submission is possible from 2027-03-19, and only with a research use such as LV-11 |
| A study of over 36,000 projects: 89 % lost their core team at least once, about 70 % of abandonment happens within three years, most projects depend on one core developer, and 27 % of the abandoned ones found a new one | [Nourry et al.](https://arxiv.org/abs/2412.00313) (2024) | page | The usual way such a project ends is the way this one is built today. LV-15 |
| An older study of 1,932 popular projects: a revival came from newcomers who used the software themselves, and the barriers were time and getting push access | [Avelino et al.](https://arxiv.org/abs/1906.08058) (2019) | page | LV-14 writes down how a maintainer gets rights |
| Two case studies of software written by coding agents appeared in 2026: a healthcare platform where agents write and review, and one engineer's build over 12 weeks. Neither reports defect counts. No published case of one maintainer and several agents building a distributed system was found, in a search that was not exhaustive | [Ferris and Bonilla](https://arxiv.org/abs/2610.08651), [Cheap code, costly judgment](https://arxiv.org/abs/2607.01087) | page | LV-12 cites both and reports counts |
| ICSE 2027 runs from 2027-04-25 to 2027-05-01 in Dublin, with a workshop on agentic engineering. Its deadlines are not on the page read | [ICSE 2027](https://conf.researchr.org/news/icse-2027) | page | A possible venue for LV-12 |
| NLnet takes proposals from individuals for EUR 5,000 to 50,000, with the next deadline on 2027-01-03. A project needs a strong European dimension: as a rule, people in Horizon Europe countries receive at least 60 % of the funds, and other proposals need exceptional quality. Its policy of 2026-10-20 forbids generative AI in writing a proposal, including for ideas and translation, and in producing any artefact of funded work: code, design or documentation. It allows read-only use, such as a tool that finds a defect which a person then fixes. A proposal must say whether the code base holds a significant amount of AI-generated code | [Proposal form](https://nlnet.nl/propose/), [policy 2.0](https://nlnet.nl/foundation/policies/generativeAI/2.0/), [eligibility](https://nlnet.nl/codesupply/eligibility/) | page | A poor fit for a project that coding agents write, run from Vietnam. A grant would mean that the owner writes the proposal and the funded work by hand, and declares how the code base was written. LV-16 |
| GitHub Sponsors supports Vietnam. The GitHub Secure Open Source Fund pays USD 10,000 per project and asks for clear governance. ISIF Asia funds registered organisations only | [GitHub Sponsors](https://docs.github.com/en/sponsors), [Secure Open Source Fund](https://github.com/open-source/github-secure-open-source-fund), [ISIF Asia](https://apnic.foundation/grants/isif-asia/) | page | LV-16. LV-14 comes first, because a fund asks for governance |
| Software Freedom Conservancy expects a community and, as a rule, a project older than a year | [Conservancy](https://sfconservancy.org/projects/apply/) | page | No home is sought before maintainers from outside exist |

Not checked: programmes in Vietnam that fund student or open-source projects, the Software
Heritage pages, and any published price of a security audit.

### What this adds to the existing plan

- Design 0054 asks for fuzzing of every parser and for an independent review, both before the
  public pool. This record starts the fuzzing now and names the review as an owner item.
- NET-7 plans an open specification at stage H4. This record writes the specification of the
  cluster protocol now, because the protocol exists and the golden-bytes tests already fix it.
- ECO-8 plans a documentation site, a governance file and a code of conduct. This record orders
  them with the rest and adds the test that they work: a release without the owner.
- PROOF-3 of design 0056 makes the release reproducible. This record adds provenance that anyone
  can verify on the same archives.

## Goals

- **G1** A stranger can check, from published evidence, how the grid is tested and what it
  guarantees.
- **G2** A decoder written from the specification alone reads every message of the protocol.
- **G3** A release can be cited, verified and rebuilt years later.
- **G4** The project gets through one release cycle without its founder.
- **G5** The way the project is built (one owner, coding agents, evidence rules) is documented
  well enough for others to reuse or to criticise.

## Non-goals

- Publicity. Nothing in this record is a launch or a campaign.
- Features. They belong to the roadmap and to design 0056.
- A proof of the whole code base. The model of LV-3 covers one state machine and says what it
  leaves out.
- A foundation or a legal entity before the project has maintainers from outside.

## Design

### D1 The rule and the order

- Every release ships at least one item of this record (Q31).
- The record takes one lane of the agents' work at most. Design 0056 Q26 splits the rest.
- Order is by value for effort (D7). Items the owner must do in person (an application, an
  account, a signature) are marked "Owner" and can start at any time.
- An item is finished when its check runs in CI or its proof run (D6) is recorded.

### D2 Assurance

- **LV-1 Fuzzing.**
  - One fuzz target for every parser of bytes that another party wrote: the frame reader and its
    format dispatch, the handshake frames, `InboundMessage` in both encodings, the encrypted record
    reader, the join token and the invite, `TreeManifest`, the journal reader, and the JSON-RPC
    reader of `oxideswarm mcp`.
  - The targets live in `fuzz/`. CI runs each for a short time on every pull request and longer at
    night.
  - A crash becomes a regression input in the repository, and a security finding goes through the
    private reporting route first.
  - Design 0054 asks for this before the public pool. Here it starts now.
- **LV-2 Deterministic simulation.**
  - A harness runs one master and many workers in one process, on `GridStream::Mock` links and
    paused time, from one seed. The same seed gives the same run.
  - It injects faults: a lost link, a slow link, a worker that stops, a worker that returns under
    a new session, and a master restart with journal replay.
  - It injects faults into the journal as well: a write or a sync that fails at a chosen count,
    and a stop in the middle of a write, each followed by a replay.
  - After each run it checks invariants:
    - no accepted task is lost;
    - a task reaches one terminal state, once;
    - a task is never assigned to two live sessions at the same time;
    - every state change is one that `can_transition_to` allows;
    - retries and evacuations stay inside their budgets;
    - when the faults stop, the queue drains within a bound.
  - CI runs a fixed set of seeds on every pull request and many more at night. A failing seed is
    kept as a test.
  - R11 decides between a harness of our own and a library.
- **LV-3 A checked model.**
  - A model of the task state machine, session ownership, the failover sweeps and journal replay,
    in TLA+ or Quint, with the invariants of LV-2.
  - The model lives in the repository and CI checks it. The record of this item states what the
    model leaves out.
  - It makes the delivery guarantee exact: a document says precisely when a task can run twice.
- **LV-4 An inventory of `unsafe`.**
  - Every `unsafe` site gets a `SAFETY:` comment that states the invariant it relies on, enforced
    by the clippy lint `undocumented_unsafe_blocks`.
  - A module without `unsafe` forbids it.
  - Miri runs on the unit tests it can run, which excludes calls into the operating system.
- **LV-5 Supply chain.**
  - `cargo deny` checks licences, banned and duplicated crates, and sources.
  - Each release archive gets build provenance that anyone can verify (`actions/attest`), next to
    `SHA256SUMS`, with the verify command in `README.md`.
  - The binaries carry their dependency list.
  - Actions in the workflows are pinned by commit, and their tokens are read-only by default.
  - Two outside checklists score the repository: OpenSSF Scorecard in CI, and the OpenSSF Best
    Practices badge at its first level.
  - This joins PROOF-3 (a reproducible release) and ECO-5 (signed and notarized binaries).
- **LV-6 An independent review.** Owner.
  - People outside the project review the handshake of ENROLL-1, the isolation layer and the
    parsers. The findings and the fixes are published.
  - Before a review, design 0051 states the known properties of the Noise `IK` pattern, among
    them that its first message is open to key-compromise impersonation, and what OXS-AUTH v3
    sends in that message.
  - The owner applies to a programme that arranges or funds such reviews. The review comes after
    LV-1 to LV-5, so that reviewers spend their time on what tools cannot find.
  - Design 0054 requires a review before stage H2 in any case.
- **LV-7 A testing page.** `docs/TESTING.md` says what is tested and how: unit, integration,
  chaos, golden bytes, fuzzing, simulation and the model. A script in CI produces its numbers. It
  also lists what is not tested.

### D3 Specification

- **LV-8 The cluster protocol, written down.**
  - A specification in `docs/spec/`: framing, format bytes, the handshake, encrypted records, the
    message catalogue in both encodings, version negotiation, capability bits and error behaviour.
  - Test vectors as files, exported from the golden-bytes tests.
  - A second decoder, written from the specification in another language with no code from this
    repository, reads every vector in CI. A difference between the specification and the code
    fails CI.
  - This is the specification part of NET-7, done now for the protocol inside a cluster. The
    protocol between clusters stays in NET-7.
  - `docs/README.md` gains "Specification" as a document type in the same change.
- **LV-9 The formats, written down.** Blob ids and `TreeManifest`, the join token and the invite,
  and later the receipt of PROOF-1 and the charter digest of design 0057. Each has vectors. Formats
  outlive the code that first wrote them.

### D4 Evidence

- **LV-10 Citable and archived.** A `CITATION.cff`, a DOI for each release, an archived copy of the
  source, and a "How to cite" section in `README.md`.
- **LV-11 A measurement study.** The idle time and the usable compute of a school lab, from spike
  R4 and lighthouses L4 and L6 of design 0056: the method, the scripts and an anonymised dataset,
  all open. It updates the older study that design 0056 cites.
- **LV-12 A case study of the method.** How one owner and several coding agents built and reviewed
  a distributed system: design records, a backlog with a "Done when" per item, a fixed status
  vocabulary, the rule that a hardware claim needs a dated record, the review findings, and the
  agent reports that turned out false and how they were caught. The repository's history is the
  data, with counts. It reports the failures with the same weight as the successes, and it cites
  the case studies that exist.
- **LV-13 A system description.** After lighthouses L1 to L3: the architecture and the measured
  results, as a preprint. A journal for research software becomes possible once the repository has
  six months of public history and a recorded research use.

### D5 Stewardship

- **LV-14 Governance files.** A code of conduct. A `GOVERNANCE.md` that says who decides today (the
  owner, through design records), how a change is accepted, how a maintainer is added, and how the
  steward of Q17 takes over. A funding file when the owner has an account. A licence layout that
  GitHub detects. A statement in `README.md` of how coding agents are used to build the project,
  which funds and journals ask for. This is item 2 of ECO-8.
- **LV-15 A release without the owner.**
  - A runbook from tag to published archives.
  - A list of every secret, account and domain the project depends on, and who holds each.
  - A second person with the rights to merge and to release.
- **LV-16 Funding and a home.** Owner. Applications to programmes that fund open-source
  infrastructure, written by the owner in person. A sponsorship account. A fiscal host once a
  maintainer from outside exists. Each fund's rules on work written by AI are read first, because
  coding agents write this project's code and the project says so. D10 lists the candidates.
- **LV-17 The users' language.** User guides in Vietnamese beside the English ones, with English as
  the source, and the documentation site of ECO-8. Writing standard rule 1 changes for user guides
  only (Q35).

### D6 Proof runs

Each is recorded with its date, as the lighthouse runs of design 0056 are.

| ID | Name | The sentence | Needs |
|---|---|---|---|
| V1 | No task lost | The nightly simulation ran its seeds of injected faults and every invariant held, and the testing page shows the count | LV-2, LV-7 |
| V2 | A second reader | A decoder written from the specification alone reads every published vector | LV-8 |
| V3 | A release without the owner | A second person cuts a release from the runbook alone, and its archives verify | LV-15, LV-5 |
| V4 | A stranger's change | A person the owner does not know lands a change by following `CONTRIBUTING.md` alone | LV-14 |

### D7 Order

| Step | Items | Why here |
|---|---|---|
| Now, beside any wave | LV-10, LV-14, LV-5, the first targets of LV-1 | Each is size S or M and needs nothing else. In `docs/BACKLOG.md` since 2026-11-10 |
| Next | LV-2 after R11, LV-8 after R13, LV-4 | The two deep items, and the inventory they rely on |
| Then | LV-3, LV-7, LV-9, LV-15 | They build on the simulation and the specification |
| With the lighthouse runs | LV-11 after L4 or L6, LV-13 after L1 to L3 | They need the recorded data |
| Owner, at any time | LV-6, LV-12, LV-16, LV-17 | An application, a consent or a translation |

### D8 Spikes

The numbering continues from design 0056. Each spike is sized S and ends with a dated record.

| ID | Question | Method | Stop rule |
|---|---|---|---|
| R11 | Can a master and its workers run deterministically from a seed? | Run one in-memory integration test twice with the same seed and compare the event logs. List every source of difference: clocks (about 100 call sites in `crates/core` and `crates/master`), spawned threads, random values, map iteration order | If the sources cannot be controlled inside the time box, LV-2 covers the queue, the scheduler and the session layer as state machines under a simulated event loop, or uses a library that replaces the runtime |
| R12 | What does one hour of fuzzing the frame reader and the message decoders find? | Two targets, one hour each | None. A finding is fixed before LV-1 continues |
| R13 | Can a person write a decoder from a draft of the specification alone? | Draft the framing and five messages. A second agent that has not read the code writes the decoder | Each question the second agent has to ask is a gap that the specification closes |

### D9 Measures

The numbering continues from design 0056. Each number comes from a script in CI.

| ID | Measure | Target |
|---|---|---|
| M9 | Simulation seeds per night, and invariant violations found so far, each with its seed | Reported, no target |
| M10 | Fuzz targets, and crashes found and fixed | Every parser in LV-1 has a target |
| M11 | `unsafe` sites with a `SAFETY:` comment | 100 % |
| M12 | Release archives with provenance that verifies | 100 % |
| M13 | People who can cut a release | At least two |
| M14 | People outside the project with a merged change | Reported, no target |

### D10 Funding and support candidates

Checked on 2026-11-10 for LV-16, after NLnet was ruled out. The owner is an individual in Vietnam
without a legal entity, and coding agents write the project's code. "Rule on AI" is what the
programme's own page says. "None found" means that the page read has no such rule, and the form is
checked again at the time of applying. Every application states how the project is built. "Basis"
is used as in the Context.

Usable now:

| Source | What it gives | Conditions | Rule on AI | First step | Basis |
|---|---|---|---|---|---|
| [GitHub Sponsors](https://docs.github.com/en/sponsors) | Sponsorships to a personal account, with no fee | Vietnam is a supported region. Two-factor sign-in, a bank account in Vietnam and the tax form W-8BEN. No legal entity | None found | Apply from the Sponsors dashboard, then add the funding file of LV-14 | page |
| [GitHub Student Developer Pack](https://education.github.com/pack) | Partner offers and free tools for verified students | Student verification. Whether the owner's school qualifies is not checked | None found | Verify on the pack's page | page |
| [Codex for Open Source](https://developers.openai.com/codex/community/codex-for-oss) (OpenAI) | Six months of a paid plan, and API credits through a fund | Meant for core maintainers of widely used projects. Others may apply and explain their role. Rolling | None found | The form on the programme's page | page |
| [Claude for Open Source](https://www.anthropic.com/claude-for-oss-terms) (Anthropic) | Six months of a paid plan | Usage thresholds that this project does not meet. A second track is at Anthropic's discretion. Rolling | None found. It may discount automated activity | Apply on the discretionary track | page |
| [Z Fellows](https://zfellows.com) | One week of mentoring. USD 10,000 is optional and comes as an instrument that converts to equity | Individuals anywhere, students included, no company. Rolling | None found | The application form. The money can be declined | page |

Worth preparing for:

| Source | What it gives | Conditions | Rule on AI | First step | Basis |
|---|---|---|---|---|---|
| [Foresight Institute, "Local Compute" call](https://foresight.org/?p=13213) | USD 30,000 to 100,000 | Individuals worldwide. Everything is open source. It funds community-owned compute, node software and pilot deployments. The 2026 call closed on 2026-10-31 | None found. The call welcomes AI-first work | Ask for the date of a 2027 call | page |
| [O'Shaughnessy Fellowships and Grants](https://osvfellowship.com) | USD 10,000 to 100,000, with no equity | Adults worldwide. The 2026 cycle ran from 2026-01-01 to 2026-04-30. No 2027 date is published | None found | Watch for the 2027 cycle | page |
| [aigrant.org](https://aigrant.org) open-source grants | USD 5,000 to 50,000 in compute or cash | Individuals are among past grantees. The page gives no process and no dates | None found | Ask how to apply | page |
| [Emergent Ventures](https://mercatus.org/emergent-ventures) | A grant. The page states no amount | Age 13 and over, worldwide, rolling | It refuses applications written by AI, beyond proofreading | The owner writes the application in person | page |
| [GitHub Secure Open Source Fund](https://github.com/open-source/github-secure-open-source-fund) | USD 10,000 and cloud credits, with a security programme | A maintainer aged 18 or over in a Sponsors region. It asks for traction, an OSI licence and clear governance. Rolling | None found | After LV-14 and LV-5, once the project has users | page |

In Vietnam, for recognition more than for money. Each goes through a school:

| Source | What it gives | Conditions | Rule on AI | First step | Basis |
|---|---|---|---|---|---|
| [Olympic Tin hoc Sinh vien, open-source software track](https://vfossa.vn/tin-tuc/the-le-olp-phan-mem-nguon-mo-nam-2026-760.html), with VFOSSA | A national student award | Teams of up to three with a lecturer, registered by a university or a college. Each edition sets a theme. The 2026 edition builds on a given product and has its final on 2026-12-10. Whether the owner's school counts is not stated | None found | Ask VFOSSA whether the owner's school may register a team in 2027 | page |
| Eureka (Thanh Doan TP.HCM) and SV-STARTUP (Ministry of Education and Training) | Student awards | Projects are selected through participating schools. No 2027 dates are announced | None found | Ask the school whether it takes part | summary |

Ruled out, each for one reason:

- NLnet: its policy on generative AI, and the European dimension.
- Sovereign Tech Fund: work of at least EUR 50,000 on base technology.
- Alpha-Omega: it asks for a case as critical infrastructure.
- FLOSS/fund: it does not consider projects with minimal usage for now.
- Open Technology Fund: it is not accepting proposals, and an earlier round asked for software
  older than three years.
- Microsoft Imagine Cup 2027: it needs a registered company.
- 1517 Medici grants: teams in North America.
- Thiel Fellowship: for people aged 22 or younger who leave school.
- Rust Foundation fellowships: no open round.
- Accelerators such as Y Combinator, AI Grant and Antler: a company, equity or a full-time
  commitment.
- The student research prize of the Ministry of Education and Training: a recognised
  higher-education institution nominates.

What the list shows: every source with a substantial amount asks for users or traction, and the
project has few. The preparation that helps each application is the plan itself: a recorded run
with real users, such as the classroom of lighthouse L4, and the first items of this record.

## Compatibility

No item changes the wire format, a CLI flag or a default. LV-8 fixes the current protocol in
writing, so a later change to the wire format must change the specification and the vectors in the
same pull request. LV-4 and LV-5 add CI jobs. They are required checks only after they pass on
`master`.

## Security

| Exposure | Rule |
|---|---|
| Fuzzing or simulation finds a vulnerability | It goes through the private reporting route of `docs/SECURITY.md`. The regression input is published with the fix, not before |
| A second person holds release rights | Branch protection, required checks and two-factor sign-in stay on. LV-15 lists every secret and who holds it |
| The case study quotes messages between the owner and the agents | Only what is already in the public repository is used without asking. Anything else needs the owner's consent (Q33) |
| An outside review finds defects | The findings are published after the fixes, with dates |
| Provenance is trusted more than it deserves | `README.md` says what it proves: where and from which commit an archive was built. It does not prove that the source is safe |

## Test plan

- LV-1: each target runs in CI, and a planted malformed frame is found by its target within the
  pull request's time budget.
- LV-2: a test plants a defect (a task dropped at a failover) and the simulation finds it within
  the fixed seeds.
- LV-3: the model checker fails on a planted violation of an invariant.
- LV-5: a release archive with one changed byte fails the verify command.
- LV-8: a vector changed by one byte fails the second decoder, and a message added to the code
  without a vector fails CI.
- The proof runs of D6, each as a dated record.

## Work items

"Needs" lists items of this record, of design 0056 and of the backlog. Sizes are the backlog's S,
M and L. LV-1 (its first targets), LV-5, LV-10 and LV-14 are in [BACKLOG.md](../BACKLOG.md) since
2026-11-10, with their files and checks. The other items enter it when the owner adds them.

| ID | Item | Track | Size | Needs | Files | Done when | Status |
|---|---|---|---|---|---|---|---|
| LV-1 | Fuzz targets for every parser, in CI | Assurance | M | R12 | `fuzz/` (new), `.github/workflows/ci.yml` | Measure M10 is met, and the LV-1 test of the test plan passes | Planned |
| LV-2 | Deterministic simulation of master and workers | Assurance | L | R11 | `crates/master/`, `crates/core/src/transport.rs`, `tests/` | The LV-2 test of the test plan passes, and proof run V1 is recorded | Planned |
| LV-3 | A checked model of the task state machine and failover | Assurance | M | LV-2 | a new directory, `docs/design/` | CI checks the model, and the LV-3 test passes | Planned |
| LV-4 | Inventory of `unsafe`, with comments, lints and Miri | Assurance | M | | `crates/*/src/`, `.github/workflows/ci.yml` | Measure M11 is met, and the lint is a required check | Planned |
| LV-5 | Supply chain: dependency policy, provenance, outside checklist | Assurance | M | | `deny.toml` (new), `.github/workflows/release.yml`, `README.md` | Measure M12 is met, and the LV-5 test passes | Planned |
| LV-6 | Independent security review | Assurance | Owner | LV-1 | `docs/SECURITY.md` | The findings and the fixes are published with their dates | Planned |
| LV-7 | Testing page with numbers from a script | Assurance | S | LV-1, LV-2 | `docs/TESTING.md` (new), `scripts/` | The page is built in CI and linked from `README.md` | Planned |
| LV-8 | Cluster protocol specification, vectors and a second decoder | Specification | L | R13 | `docs/spec/` (new), `tests/`, `docs/README.md` | Proof run V2 is recorded, and the LV-8 tests pass | Planned |
| LV-9 | Format specifications with vectors | Specification | M | LV-8 | `docs/spec/` | Each listed format has a section and vectors that the second decoder reads | Planned |
| LV-10 | Citation file, DOI per release, archived source | Evidence | S | | `CITATION.cff` (new), `README.md` | The next release has a DOI, and `README.md` says how to cite it | Planned |
| LV-11 | Measurement study of a school lab | Evidence | M | R4, L4 or L6 of design 0056 | `benchmarks/`, a paper | The dataset and the scripts are public, and the text is posted | Planned |
| LV-12 | Case study of the development method | Evidence | M | Q33 | a paper, `docs/guides/` | The text is posted with the data it rests on | Planned |
| LV-13 | System description | Evidence | M | L1 to L3 of design 0056 | a paper | The preprint is posted | Planned |
| LV-14 | Governance files and a detectable licence | Stewardship | S | | repository root, `.github/` | The files exist and are linked from `README.md`. Proof run V4 is recorded when it happens | Planned |
| LV-15 | Release runbook, secret inventory, a second releaser | Stewardship | M | LV-5 | `docs/guides/` | Proof run V3 is recorded | Planned |
| LV-16 | Funding applications and a fiscal host | Stewardship | Owner | LV-14 | none | One application is submitted, and its outcome is in this record's status log | Planned |
| LV-17 | Vietnamese user guides and the documentation site | Stewardship | M | Q35, ECO-8 | `docs/` | The quick start and the two-machine guide exist in Vietnamese, and the site builds in CI | Planned |
| R11 to R13 | Spikes (D8) | all | S each | | scratch branches | The dated record and the result are in this record's status log | Planned |

## Decisions for the owner

The right column is a recommended default, not a decision, except where it names a date. They are
listed in [ROADMAP.md](../ROADMAP.md) as Q31 to Q35.

| ID | Question | Recommended default |
|---|---|---|
| Q31 | Accept this record and its rule: every release ships at least one of its items, and it takes one lane at most? | Yes |
| Q32 | Which items come first? | Decided on 2026-11-10 for the first items: LV-10, LV-14, LV-5 and the first targets of LV-1 are in the backlog. Recommended next: LV-2 and LV-8 |
| Q33 | May the repository's history and review records be the data of the case study (LV-12)? | Yes for what is already public in the repository. Messages outside it only after the owner has read them |
| Q34 | Will the owner apply for an independent review and for funding (LV-6, LV-16)? | Owner |
| Q35 | May user guides be translated into Vietnamese, with English as the source? | Yes. Writing standard rule 1 then says so |

## Risks

| Risk | Mitigation |
|---|---|
| The record becomes a second roadmap that competes with features | The rule of D1: one lane at most, one item per release at least |
| Simulation that is not deterministic gives false confidence | R11 first. A seed that does not reproduce is a defect of the harness and blocks LV-2 |
| A specification that drifts from the code | The second decoder and the vectors run in CI |
| Evidence is written to impress | The testing page lists what is not tested, and the case study reports the failures |
| The project still depends on one person | LV-15 and proof run V3. Until V3 is recorded, the documents say that one person maintains the project |
| A funded review or a grant is refused | LV-1 to LV-5 need no money and come first |
| A fund or a journal restricts work written by AI | Its rules are read before any application. `README.md` states how the project is built, and the owner applies only where that is accepted |

## Status log

- 2026-11-10: Draft, from the owner's request for further directions that make the project worth
  remembering. Decisions Q31 to Q35 are open. Nothing is built.
- 2026-11-10: The owner put the first items into the backlog: LV-1 (first targets, with spike R12
  as one of its steps), LV-5, LV-10 and LV-14, as Phase 10 of `docs/BACKLOG.md`. Q32 is decided
  for these. Q31 and Q33 to Q35 stay open.
- 2026-11-10: NLnet's policy of 2026-10-20 was read when the owner asked about applying. It
  forbids generative AI in a proposal and in the artefacts of funded work. The research table,
  LV-14, LV-16 and the risks now say so.
- 2026-11-10: The owner asked for other funds. D10 lists the candidates that three research runs
  checked the same day, with each programme's rule on AI and the sources that are ruled out.
