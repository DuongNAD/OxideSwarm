# 0094: The road to a shared research grid

Status: Draft (plan; builds nothing; Q94 to Q97 are open)
Created: 2026-11-19    Updated: 2026-11-19

## Summary

The owner's ultimate goal, stated on 2026-11-19, is that the people who use OxideSwarm pool their
machines to research one problem together, as volunteer computing projects do. This record frames
the road as research campaigns that people join, in four stages from today's personal clusters to
an open research grid, each with an exit check that a dated record can show. It orders the items of
Phase 9 and of [design 0054](0054-open-compute-network.md) by stage instead of adding a second
plan, and it adds four items for Stage 1 (ECO-9, NET-9, NET-10, APP-6). Money, tokens and payments
stay out, and credits only display. It builds nothing; Q94 to Q97 are open.

## Context

### The goal

- On 2026-11-19 the owner stated the ultimate goal: mobilise the people who use OxideSwarm to pool
  their hardware and research one problem together. The owner asked for this record and its first
  steps the same day ("viết design record đó đi và bắt đầu thực hiện từng bước").
- Coding agents on one owner's machines (Q1) are the first step on this road. They stay the purpose
  against which "finished" is measured (Q82, [design 0088](0088-evaluation-of-2026-11-17-and-what-finished-means.md)).
- The model is volunteer computing, as in BOINC and Folding@home (design 0054, Context), run on
  OxideSwarm. Each person keeps their own cluster (ecosystem model A, Q13).
- The owner agreed in chat that credits only display: they cannot be transferred and have no cash
  value. Q14 and Q43 are still open in [ROADMAP.md](../ROADMAP.md), so Q96 asks for the formal
  decision.

### What exists

The state at `master` `f77eabd` on 2026-11-19, release `0.4.3`.

| Part | State | Where |
|---|---|---|
| One cluster per person | A master and workers on macOS, Windows and Linux; tasks with files, isolation and workspaces | `README.md` |
| A key per machine, invites, revocation | ENROLL-1a and ENROLL-1b are Implemented. `members revoke` closes every lane of a member (ENROLL-1c, first part). An invite grants the role `worker` unless `--roles` says otherwise, and it carries the P2P ticket when the master runs `--p2p` ([design 0051](0051-machine-identity-and-invites.md) D7) | `crates/core/src/noise_auth.rs`, `crates/master/src/members.rs`, `crates/cli/src/members_cmd.rs` |
| A join from another network | Built, with no recorded run. The "Done when" of ENROLL-1 and item ENROLL-1d ask for one | ENROLL-1, USB-1c, LINK-1 |
| Task isolation | `--isolation fs` and `fs+net` on macOS, Linux and Windows ([design 0033](0033-os-level-task-isolation.md)) | `crates/worker/src/isolation.rs` |
| Run again and compare | `submit --verify <N>` runs N replicas on the first worker's operating system and architecture and compares the declared outputs by BLAKE3 (VERIFY-1, VERIFY-2). It is client side | `crates/cli/src/verify.rs` |
| A workload whose results can be checked | Anima Engine trials end in a checksum. One trial gives the same checksum on repeat runs of one system and different checksums on macOS arm64 and Windows x86_64 ([design 0089](0089-anima-engine-first-workload.md) D6) | APP-4 (Partial), BENCH-3 (Partial) |
| Limits a contributor sets | `worker --max-concurrency`, `--task-priority` (`normal`, `below-normal` or `idle`, default `below-normal`) and `--isolation` | `WorkerArgs` in `crates/cli/src/cli_app.rs` |
| Limits the master's owner sets | The host CPU threshold for new tasks is the master's `--max-host-cpu-pct` (default 85). The master's scheduler lowers a worker's concurrency by battery tier from the telemetry the worker reports (`calculate_effective_concurrency`) | `crates/master/src/scheduler.rs`, `crates/core/src/capabilities.rs` |
| A local stop | Only ending the worker process. Its signal handler ends every task process tree | `terminate_all_task_process_trees` in `crates/worker/src/process_tree.rs` |
| Fair share | Weighted round robin per submitter. The submitter is a new id for every client connection, and it is not persisted | `crates/master/src/fair_share.rs`, `crates/master/src/server/client_and_members.rs` |
| Who submitted a task | Not stored in the queue or the journal. Step 2 of revocation in design 0051 D5 needs it and is Planned, as a store format change | ENROLL-1c |
| A person | No such thing. A member is one machine key (`MemberEntry`: id, name, key, roles, times) | `crates/core/src/noise_auth.rs` |
| Credits or usage records | None. Q39 (open, recommended yes) keeps the rule that the binaries send no usage data | |

### What earlier records plan

- **Design 0054 and Phase 9 of [BACKLOG.md](../BACKLOG.md):** an open compute network in stages H0
  to H4, items NET-1 to NET-8. Q14 to Q18 are open.
- **[Design 0056](0056-frontier-program.md):** NET-1 earlier, in trusted clusters (Q21); quorum
  runs with `--distinct owner` as the redundancy part of NET-3 built early (PROOF-2); idle-aware
  contribution on desktops (GREEN-1); independent rebuilds as a first public workload (PROOF-5).
- **[Design 0057](0057-open-results-charter.md):** a project that receives donated compute
  publishes its outputs and its task code. The owner accepted the principle (Q27 to Q29).
- **[Design 0059](0059-first-users.md):** a person installs the grid when a job they already have
  ends sooner, and pooled compute alone is not such a job. A campaign gives a volunteer another
  reason: the research problem itself.
- **[Design 0061](0061-first-application-model-tunnel.md):** usage records and a balance per member
  for model tunnels (LLM-10, LLM-11), display only in the first version (Q43, open).
- **Design 0088 D1:** Phase H and the quotas of Phase G are outside the stop line. This record does
  not move the line.
- **Q80:** a stability release comes before any item of `0.5.0` is built.

### What a group of people lacks today

- A cluster cannot tell two people apart, only machines (`MemberEntry`).
- Nothing limits one person's share of the slots, and the fair share grows with the number of
  connections a person opens.
- The limits that protect a machine belong to the master's owner, not to the machine's owner.
- Nobody can see what their machines gave or what their tasks took.
- There is no campaign: no written workload, input set and verification rule that people agree to
  before they attach a machine.
- Verification is client side, and its replicas do not take the owner of a machine into account.
- Strangers' code has no sandbox beyond `--isolation`, which limits files, not intent (design 0054
  D2).

## Goals

- **G1** A person joins a research effort by attaching a machine, under limits that they set on that
  machine and that the cluster cannot raise.
- **G2** A campaign's results can be checked, and the checks grow with who may be hostile at each
  stage.
- **G3** Each stage is useful alone and ends with an exit check that a dated record shows.
- **G4** The contributor's safety and privacy come first (design 0054 G1).
- **G5** The road reuses and orders the items of Phase 9 and design 0054. It adds an item only where
  a stage needs one that no record has.

## Non-goals

- Money, tokens, payments, and any market for compute or for credits (D8).
- A pool where any stranger submits any task to any volunteer. In this record strangers meet only
  through a campaign, whose researcher alone submits its tasks.
- Choosing the science. D1 states what a campaign's workload needs, and the owner chooses (Q94).
- Moving the stop line of design 0088. Every item of this record is outside it.
- Tightly coupled work over the internet, as in design 0054.

## Design

### D1 Campaigns

A campaign is the unit that people join. It has:

- **a researcher:** the person who runs the campaign's master and who alone submits its tasks;
- **a workload:** one program at a fixed version, named by its content hash;
- **an input set:** the list of tasks, each a set of content-addressed inputs
  ([design 0006](0006-content-addressed-data-plane.md));
- **a verification rule:** how a result is accepted. Replicas that must agree (how many, and on which
  machines), unannounced tasks with known answers, or a check that the workload declares;
- **a results rule:** where results go, who sees them and under which licence. A campaign on
  donated compute follows the charter of design 0057;
- **its needs and grants:** CPU or GPU, memory, time per task, and network (none by default).

A volunteer attaches a machine to a campaign, not to arbitrary tasks. Before attaching, the volunteer
reads the campaign's sheet, and the machine then runs only that workload.

In Stage 1 a campaign is a written sheet, committed before the run, and a driver that calls
`submit`. Nothing in the code knows about campaigns. Stage 2 makes the campaign an object of the
master.

A volunteer attaches a machine by running a worker for the campaign, enrolled through the campaign's
invite with the role `worker`. It runs beside any worker of the volunteer's own cluster, because a
worker process serves one master. No federation between clusters is needed (D7).

### D2 The stages

| Stage | Who takes part | What it adds | Items | Exit check |
|---|---|---|---|---|
| 0, personal clusters (now) | One person and their own machines | The stable `0.4.x`, the public launch, coding agents as the first use | The stability items of Q80, LAUNCH-1 to LAUNCH-5, AX-1 to AX-3, CACHE-1 | The "Done when" of LAUNCH-1 holds |
| 1, trusted circles | Friends or a lab who know each other, in one cluster, from several homes | Participants, quotas, contributor controls, credits shown, a recorded join from another network, a first pilot campaign | ECO-9, NET-9, NET-10, APP-6; ENROLL-1 (step 2 of ENROLL-1c, ENROLL-1d) | About five owners and ten machines complete one campaign, and every result is verified |
| 2, research campaigns | Volunteers who do not know the researcher | The campaign object, WebAssembly tasks, k-of-n replicas and spot checks, the rest of the contributor controls, a contribution board | NET-1, NET-3, NET-4, PROOF-2; the campaign object and the board (no ids yet) | About 100 volunteers attach machines; a planted cheating volunteer is caught and none of its results is accepted; a campaign completes |
| 3, an open research grid | Anyone, and many researchers | Many campaigns, discovery without a central server, native tasks in VMs, an open specification and a steward, federation | NET-2, NET-5, NET-6, NET-7; NET-8 later | The exit checks of H3 and H4 of design 0054, and two campaigns of different researchers that run at once on volunteers who found them without a central server |

### D3 Stage 0: personal clusters

- One person runs a cluster on their own machines, and coding agents are the first use (Q1).
- What remains: the stability release (Q80), the public launch (LAUNCH-1 to LAUNCH-5) and bet A in
  release `0.6.0` (Q81).
- People lend machines only to software that already works on their own. A stable base therefore
  comes first.
- **Exit check:** the "Done when" of LAUNCH-1 holds.

### D4 Stage 1: trusted circles

A circle is a group of people who know each other, such as friends or a lab. They join one cluster
from several homes. One of them, the organiser, runs its master. Each of them keeps their own
cluster and adds a worker process to the circle's cluster.

1. **Participants.** A participant is a person. A label on each member groups that person's machines
   (ECO-9, step 1).
2. **Joining from another network.** Built: the invite carries the P2P ticket (design 0051 D7).
   Still to do: a recorded run in which a machine on another network joins with an invite sent by
   chat. It is part of ENROLL-1 (its "Done when", and ENROLL-1d) and needs no new item.
   Contributors join with the default role `worker`. Only a participant who also submits gets
   `client`.
3. **Per-participant quotas (ECO-9).** Caps on running tasks, slots held and queued tasks, and a
   weight in the fair share. They are keyed on the participant, so a share does not grow with the
   number of connections. The master needs the submitter of each task in the queue and the journal.
   That is the same change as step 2 of revocation in design 0051 D5, so one store change serves
   both.
4. **Contributor controls (NET-9).** Set in the contributor's own configuration and enforced by the
   worker, so the organiser's master cannot raise them: a host CPU threshold of the worker's own,
   hours (a weekly schedule and a daily cap), no new task on battery, no new task on a metered
   network, and a local stop switch that ends running tasks within 5 s and hands them back. It is
   the first part of NET-4, and it takes the schedule and mains-power parts of GREEN-1.
5. **Contribution credits (NET-10).** Per participant: the work their machines ran for others and the
   work their tasks used. They only display (D8).
6. **The pilot campaign (APP-6).** An Anima Engine parameter sweep by the recommendation of Q94,
   verified with `submit --verify` on the same system (VERIFY-2). The participants are Q95. A dry run
   with two or three participants on a released `0.4.x` builds nothing and comes first.

In Stage 1 the master counts completed tasks for credits. Verification is client side, so the
master does not know which results were verified; the pilot's record does.

A worker serves one master, so a machine in its owner's cluster and in a circle runs two worker
processes. The controls of NET-9 hold per worker process. Limits across all worker processes of one
machine belong to NET-4 in Stage 2.

**Exit check:** about five owners and ten machines, on at least two networks, complete one campaign
in which every result is verified by a replica on another machine. A dated record lists the
controls each contributor set and the credits shown.

### D5 Stage 2: research campaigns

- **The campaign object.** The master holds the sheet of D1, makes tasks from the input set, accepts
  results by the verification rule, and shows the progress.
- **Attaching.** A volunteer uses the campaign's invite, with many uses and the role `worker`. The
  researcher chooses whether each machine needs approval.
- **WebAssembly tasks (NET-1).** A stranger's code runs on a volunteer's machine only as
  WebAssembly, with memory and CPU fuel caps, file grants and no network. Its deterministic mode lets
  replicas on different systems compare by hash, which native Anima Engine trials cannot do (design
  0089 D6). Spike R1 of design 0056 first measures whether results are bit-identical across
  `x86_64` and `aarch64`.
- **Replicas and spot checks (NET-3).** k-of-n replicas on machines of different participants
  (PROOF-2 with `--distinct owner` is this part, built early), unannounced tasks with known answers,
  and a reputation per key. A new key starts with low trust, so a person with many keys gains
  little.
- **The rest of the contributor controls (NET-4).** Opt-in per campaign, a policy of task classes
  (WebAssembly only, the program hash of the sheet), and limits across all worker processes of one
  machine.
- **A contribution board.** Per volunteer, under a name they choose, the verified work they gave to
  each campaign. It only displays (D8).
- **Exit check:** about 100 volunteers attach machines to one campaign. A planted cheating volunteer
  is caught, and none of its results is accepted. The campaign completes, and its results are
  published under the charter of design 0057.

The campaign object and the board get item ids when the exit check of Stage 1 holds.

### D6 Stage 3: an open research grid

- Many campaigns from many researchers run at once, and a volunteer chooses which to join.
- Campaigns publish their sheets in a distributed directory over iroh, with no central server
  (NET-6).
- Workloads that cannot be WebAssembly run in throw-away VMs (NET-5).
- An open specification, conformance tests, a security process and a neutral steward (NET-7, with
  LV-8 and LV-9 of [design 0058](0058-lasting-value.md)).
- Federation between clusters (NET-2), for a lab whose cluster lends its idle machines to a campaign
  as a whole.
- Trusted execution environments (NET-8) come after this stage.
- **Exit check:** the exit checks of H3 and H4 of design 0054, and two campaigns of different
  researchers that run at the same time on volunteers who found them through the directory.

### D7 How this orders Phase 9 and design 0054

Design 0054 stays the architecture. This record changes the order of its stages and the frame of its
public pool.

| Design 0054 stage | Items | Stage here | Change |
|---|---|---|---|
| H0 | NET-1 | 2 | None in content. It is a prerequisite of Stage 2. Q21 (NET-1 earlier, in trusted clusters) stays open and fits this order |
| H1 | NET-2 | 3 | Later. Volunteers' workers attach to a campaign directly (D1), so Stage 2 needs no federation. With credits that only display (D8), the signed accounting of NET-2 becomes a signed record of work, not an exchange |
| H2 | NET-3, NET-4 | 2; the first part of NET-4 in 1, as NET-9 | The public pool becomes campaigns: one submitter per campaign, and volunteers attach to a campaign |
| H3 | NET-5 | 3 | None |
| H4 | NET-6, NET-7 | 3 | NET-6 publishes campaign sheets as well as cluster offers |
| Later | NET-8 | After 3 | None |
| Phase G | ECO-3 | 1, as ECO-9 | ECO-9 is the participant part of ECO-3. ECO-3 keeps tenants, projects, scoped tokens and the audit log |
| Design 0056 | PROOF-2, GREEN-1, PROOF-5 | 2, 1, 2 | PROOF-2 is the redundancy of NET-3 built early. The schedule and mains-power parts of GREEN-1 go to NET-9. PROOF-5 is a candidate for the first Stage 2 campaign (Q94) |

Q18 (when Phase H starts) recommends H2 after ECO-3 and an independent security review. This record
does not decide Q18. It recommends that Stage 2 needs ECO-9 rather than all of ECO-3, and it keeps
the independent review.

### D8 Credits, and what stays out

**Credits.**

- A credit counts work that a participant's machines ran for others: the run time times the CPU
  cores the task reserved (core-seconds), and GPU-seconds apart. In Stage 1 every completed task
  counts. From Stage 2 only results that the campaign's rule accepted count.
- Credits only display. They cannot be transferred, spent, sold or exchanged, they have no cash
  value, and no rule of admission or placement reads them (Q96).
- Each credit has an equal debit of the participant whose task used the work (design 0054 D6), so the
  sum over all participants is zero.
- Credits stay on the campaign's master. Nothing sends them elsewhere (Q39).
- NET-10 and the usage records of design 0061 (LLM-10, LLM-11) use one record format. The item that
  lands first defines it.

**What stays out: money, tokens and payments.**

- **Not needed.** Every participant uses their own machines and lends spare time to a shared
  problem. Nothing has to be settled between them.
- **An incentive to cheat.** A reward with a cash value pays for fake results: a dishonest
  contributor gains by returning cheap, wrong answers. Each campaign must then raise k and the rate of
  spot checks, which spends the volunteers' compute on checks. Credits that only display still reward
  vanity, so from Stage 2 they count only verified work, and the exit check of Stage 2 plants a
  cheater.
- **Law and fraud.** Payments and tokens bring payment law, tax and fraud into the project, and for
  tokens the rules on securities and crypto assets (design 0054 D6, design 0061 Context). A project
  run by one person cannot carry them.
- **The charter.** Design 0057 treats compute as donated and its outputs as open. Paid compute would
  be a market, not a donation.

Q14 stays the place where this is decided. Q96 asks for the decision.

### D9 Threat model by stage

| Stage | Who may be hostile | New risks | Answers |
|---|---|---|---|
| 0 | No one inside the cluster; attackers on the network | As today (`docs/SECURITY.md`) | The handshake, encrypted records, isolation |
| 1 | No one by intent; people and machines make mistakes | (a) One participant's tasks run on another participant's machine: a member with `client` can run commands on every worker, within `--isolation` (design 0051, Non-goals). (b) A broken machine (bad memory, an overclock) returns wrong results. (c) The organiser's master sees each machine's name, system, hardware, battery, load, address and hours, and every member can list the workers. (d) A participant leaves on bad terms | (a) Contributors join as `worker` only, the organiser alone holds `client` in the pilot, and contributed workers run `--isolation fs+net` where the workload works under it, `fs` otherwise. (b) Every result is verified by a replica (VERIFY-2). (c) The campaign sheet says what the cluster learns about a machine; owners choose their workers' names; credits show participant labels only; nothing leaves the master (Q39). (d) `members revoke` closes every lane (ENROLL-1c) |
| 2 | From a volunteer's side, the researcher's code. From the researcher's side, some volunteers | (a) A stranger's code on a volunteer's machine. (b) Volunteers who return wrong results on purpose, alone or with many keys. (c) The researcher learns volunteers' addresses, hardware and online hours. (d) A campaign that is not research, such as mining or scanning | (a) WebAssembly only, no network, fuel and memory caps (NET-1); the program hash is fixed in the sheet; the policy of NET-4. (b) Replicas on different participants, spot checks, reputation, low trust for new keys (NET-3). (c) A board under a chosen name, a relay for volunteers who want one, telemetry limited to what scheduling needs, and a volunteer's records deleted when they leave. (d) Task classes (NET-4) and the charter: the code and the outputs are published |
| 3 | Anyone on the internet | Escapes from VMs, false campaigns and spam in the directory, capture of the governance | Attack tests of NET-5, campaign sheets signed by keys with a history, the security process and the steward of NET-7 |

Each stage starts with its own threat model and an independent review, as design 0054 requires. When
in doubt, a task does not run.

### D10 Relation to releases 0.5.0 and 0.6.0

- Release `0.5.0` is language model serving across machines
  ([design 0067](0067-multi-node-llm-serving-and-admission.md)). It is Design only, and no item of
  it is built before the stability release (Q80).
- Stage 1 can run beside it, because its parts serve both:
  - ECO-9 limits a friend's model requests as much as a friend's tasks;
  - NET-9 protects the owner of a GPU that serves a model to the circle;
  - a join from another network serves a model tunnel (LLM-9) and a campaign alike;
  - NET-10 and LLM-10 share one record format (D8).
- Release `0.6.0` is bet A (Q81), and it serves the purpose of Q1. Stage 1 does not take its place.
  Q97 asks whether Stage 1 runs in parallel, as one lane at most.

### D11 Order of work and gates

1. Now: this record, which builds nothing. The owner answers Q94 to Q97.
2. Before the stability release (Q80), only docs-only or test-only work of Stage 1 merges: the design
   record of each item, the campaign sheet of APP-6, and tests that pin today's behaviour. The dry
   run of APP-6 on a released `0.4.x` builds nothing and may run once the owner names its
   participants (Q95).
3. After the stability release: ECO-9, with step 2 of ENROLL-1c, and NET-9, in parallel. Then
   NET-10, which needs the participant label and the submitter record of ECO-9.
4. The recorded join from another network (ENROLL-1, ENROLL-1d), whenever the owner has a second
   network. It also serves check E5 of design 0088.
5. APP-6, the pilot campaign. Its record is the exit check of Stage 1.
6. Stage 2 items get ids and a backlog phase only after the exit check of Stage 1 holds and spike R1
   of design 0056 has a record.

Each item that changes behaviour gets a design record of its own (writing standard rule 9).

## Compatibility

- **ECO-9:** the participant label is a new field of `members.json` with a default, so older files
  load. Carrying it in `ClientResponse::MembersList` is a wire change, because bincode is positional;
  it follows the release train of [design 0076](0076-protocol-compatibility-and-release-train.md) D1
  and the one-version window of [design 0083](0083-one-version-window.md). The submitter of each task
  in the journal is a store format change (`crates/master/src/store.rs`), as step 2 of ENROLL-1c
  already says.
- **NET-9:** local to the worker. It needs no wire change if the worker reports no free slot and
  hands a task back with the refusal of [design 0090](0090-sandbox-base-refusal-hand-back.md). A
  reason shown on the master is a worker tag or the next wire change.
- **NET-10:** the record stays on the master. A client request that reads it is a new message
  variant, so a wire change.
- **Stage 2:** the campaign object and `TaskSpec::Wasm` (NET-1) are wire changes.
- **Stage 3:** the network layer is a separate, versioned protocol (design 0054, Compatibility).

## Security

This record adds no input and no code. D9 is the threat model by stage. Each item has a security
section in its own design record. The contributor controls are enforced on the contributor's machine,
because in Stage 1 and later the master belongs to someone else. The safety of the contributor
outranks every other goal (design 0054, Security).

## Test plan

- Each exit check of D2, with a dated record.
- ECO-9, NET-9 and NET-10: the tests in their "Done when" in [BACKLOG.md](../BACKLOG.md).
- Stage 2: the adversarial tests of design 0054 (sandbox escapes, cheating contributors, forged
  records) and a planted cheater.
- This record: `python3 tests/test_m1_challenger_hygiene.py` (links and anchors).

## Work items

The texts are in [BACKLOG.md](../BACKLOG.md), Phase 19, where their status is kept.

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| ECO-9 | Per-participant quotas: running tasks, slots and priority (D4 step 3) | `crates/core/src/noise_auth.rs`, `crates/master/src/members.rs`, `crates/master/src/fair_share.rs`, `crates/master/src/queue.rs`, `crates/master/src/scheduler.rs`, `crates/master/src/store.rs`, `crates/cli/src/members_cmd.rs`, `README.md`, a design record | Caps and fair share per participant hold in loopback tests and survive a master restart | Planned |
| NET-9 | Contributor controls on each machine, the Stage 1 part of NET-4 (D4 step 4) | `crates/worker/src/client.rs`, `crates/worker/src/heartbeat.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/cli_app/worker.rs`, `crates/cli/src/config.rs`, `README.md`, a design record | No task starts outside the contributor's limits, and the stop switch ends tasks within 5 s, in worker tests | Planned |
| NET-10 | Contribution credits per participant, display only (D4 step 5, D8) | A ledger module under `crates/master/src/`, `crates/master/src/web_ui.rs`, `crates/master/src/dashboard.html`, `crates/cli/src/members_cmd.rs`, `README.md`, a design record | Credits match the tasks' run times, sum to zero, and survive a restart | Planned |
| APP-6 | Pilot campaign: an Anima Engine sweep on several owners' machines, every result verified (D4 step 6) | `benchmarks/campaigns/` (new): the sheet and the record | The record of the exit check of Stage 1 | Planned |

Items of other records that this road needs: ENROLL-1 (step 2 of ENROLL-1c, ENROLL-1d), APP-4 and
VERIFY-2 for Stage 1; NET-1, NET-3, NET-4 and PROOF-2 for Stage 2; NET-2, NET-5 to NET-8 for Stage 3.

## Decisions for the owner

| ID | Question | Recommended answer |
|---|---|---|
| Q94 | What is the first research problem: the workload of the Stage 1 pilot, and the rule for choosing the first Stage 2 campaign? | The pilot runs an Anima Engine parameter sweep in deterministic CPU mode: its trials end in a checksum, VERIFY-2 checks them today, and the participants know the owner. The first Stage 2 campaign is chosen after the pilot's record, among workloads that split into independent tasks of minutes, whose results a checksum or a cheap check verifies, whose code and outputs can be published under design 0057, and whose inputs hold no personal data. Candidates: Anima Engine if it then has a public release (Q52), independent rebuilds (PROOF-5), and the workloads of Q16 |
| Q95 | Who takes part in the Stage 1 pilot? | The owner and three to five people the owner already knows, for example participants of ADOPT-1 or of the USB kit trial, with about ten machines on at least two home networks and at least two machines of each operating system and architecture in use, so that VERIFY-2 finds a replica. Each person reads the campaign sheet and agrees to it before joining |
| Q96 | Are credits display only: not transferable, never spent or sold, with no cash value, in every stage of this record? | Yes. The owner agreed in chat on 2026-11-19. The answer also closes Q14 for the research grid (no money, tokens or payments) and Q43 (a balance displays and refuses nobody) |
| Q97 | Does Stage 1 run in parallel with release `0.5.0`? | Yes, as one lane at most beside `0.5.0` and bet A (`0.6.0`), with implementation merged after the stability release (Q80) and docs-only or test-only work before it. Quotas, contributor controls and joins from other networks serve model serving as well |

## Risks

| Risk | Answer |
|---|---|
| Nobody joins: pooled compute alone does not move people (design 0059) | The pilot starts with people the owner knows, and a campaign offers a problem worth joining (Q94) |
| The road crowds out the work that finishes the project | One lane at most, and implementation after the stability release (Q97) |
| A wrong result reaches a published finding | Every result is verified in the pilot, and each mismatch is explained in the record |
| Native workloads give different results on different systems (design 0089 D6) | Replicas on the same system until WebAssembly tasks (NET-1) and spike R1 show otherwise |
| The owner's time with each participant | The dry run with two or three people shows the cost before the full pilot |

## Status log

- 2026-11-19: Draft, at the owner's request of the same day. Ids claimed on the hub with
  `agent_bridge.py claim` (design 0087): design 0094, phase 19, Q94 to Q97, ECO-9, NET-9, NET-10 and
  APP-6. The four items entered [BACKLOG.md](../BACKLOG.md) as Phase 19, Planned and outside the stop
  line. Q94 to Q97 are open.
