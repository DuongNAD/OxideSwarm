# 0061: A first application for the model track: trial, client tunnel and usage records

Status: Draft (plan; builds nothing; the owner decided Q40 on 2026-11-11; Q41 to Q43 are open)
Created: 2026-11-11    Updated: 2026-11-12

## Summary

On 2026-11-11 the owner described the roles of the owner's two projects. OxideSwarm carries model
traffic between machines, secures it and keeps the account of who served whom. LIVA, the owner's
desktop assistant, gives people a reason to use that path. This record writes the roles down and
sets an order:

1. a trial of LIVA on a Mac with the language model on another machine, which needs nothing from
   the grid (APP-1);
2. a client tunnel to a model server that listens on loopback on a worker (LLM-9);
3. usage records for tunnels (LLM-10) and a balance per member (LLM-11).

It builds nothing, and no item is in the backlog.

## Context

### What the owner said

Three statements of 2026-11-11, paraphrased from Vietnamese:

1. OxideSwarm should suit LIVA, and local models in general, better than it does.
2. OxideSwarm is the transit, the security and the "currency". LIVA is what makes people use the
   network and what makes that currency move.
3. Before any of that, LIVA has to be tried on the Mac while another machine computes the
   language model.

### What OxideSwarm has

1. **No path between a client and a worker.** A client connects to the master (`connect_master`,
   `crates/cli/src/connect.rs`) and so does a worker (`WorkerClient`,
   `crates/worker/src/client.rs`).
2. **No byte stream for a client.** A client connection carries `ClientMessage` and
   `ClientResponse` frames. On QUIC it is one stream tagged `STREAM_CLIENT`
   (`crates/core/src/transport.rs`).
3. **A reply is the output of a task.** It arrives when the task ends
   ([design 0038](0038-large-language-model-inference.md), D1). Live output is AX-3 of
   [design 0056](0056-frontier-program.md), Planned.
4. **Bulk data already has a path of its own.** Blobs move in 4 MiB chunks and a `BlobService`
   (`crates/master/src/server.rs`) answers them off the inbound loop, on the QUIC data stream. A
   tunnel can follow that pattern.
5. **Clients are not told apart.** Every client holds the same cluster secret. A key per machine
   is ENROLL-1 ([design 0051](0051-machine-identity-and-invites.md)), which is not merged.
6. **The model plan.** LLM-1 is a guide: a prompt goes to the worker whose tag names the model.
   LLM-4 is a tunnel between a head node and `ggml-rpc-server` processes. LLM-6 is a gateway
   whose first version returns each reply whole (design 0056). None is built.

### What LIVA has

Checked on 2026-11-11 in LIVA's repository, at `main` `3057d35c` and at branch `mac-v2`
`866459fb`:

- **The chat model runs in the application's process.** `liva-native-core/src/boot.rs` builds one
  closure of type `LlmStreamingBackendFn` (crate `liva-llm`) around the llama.cpp engine. A model
  on another machine would be a second closure of the same type.
- **No setting for a model on another machine is read.** `cloudBaseUrl` exists in the
  configuration file and in the settings page. `AI_BASE_URL` appears only in `.env.example`.
- **An evaluation tool can already use an endpoint.** `liva-native-core/src/bin/liva_eval.rs`
  accepts `--provider cloud --api-base <URL>` and reports time to first token and tokens per
  second.
- **Nothing records that the application starts on macOS.**
  - `main` has no macOS job in its workflows and no code for `target_os = "macos"`.
  - `mac-v2` is 48 commits ahead of `main` and 51 behind. Its job `test-macos` runs `cargo test`
    and the interface tests on `macos-latest`. The job passed on 2026-08-27 at `35f5f26d` and
    failed at the branch head on 2026-09-06.
  - A comment in that job leaves the Tauri build and the GPU build of llama.cpp out of it.
- **LIVA's own plan** has the matching items: KH-1 (an endpoint setting), KH-2 (a guide for two
  machines) and KH-3 (the interface says when a question leaves the machine). They are in a
  `ROADMAP.md` on a pull request of that repository, not merged.

### What exists outside

"Basis" says how the source was read: `page` means the page itself was read on 2026-11-11,
`summary` means only a summary of it was read.

| Finding | Source | Basis | Consequence |
|---|---|---|---|
| For untrusted environments or networks, llama.cpp's security policy says not to use the RPC backend, `ggml-rpc-server` and `llama-server`, and to encrypt data sent over the network | [SECURITY.md](https://github.com/ggml-org/llama.cpp/blob/master/SECURITY.md) | page | The model server stays on loopback. The grid carries its traffic |
| An unauthenticated flaw in the RPC backend of llama.cpp was published in 2026 as CVE-2026-34159 | [Tenable](https://www.tenable.com/cve/CVE-2026-34159) | summary | LLM-4 and LLM-5 never expose that port, as design 0038 already says |
| AI Horde: volunteers run text and image models for others. A worker earns "kudos" for each finished job and for being online, kudos set the place in the queue, and its terms forbid selling or buying them | [Kudos](https://horde-sdk.readthedocs.io/en/latest/haidra-assets/docs/kudos) | page | An account that is not money works for this kind of exchange. This record reads "currency" as the credits of design 0054 |
| Load balancers with an OpenAI-compatible interface exist for model servers on a local network: LiteLLM, Olla, llamesh and the `llm-gateway` of OpenZiti | [Olla](https://thushan.github.io/olla), [llm-gateway](https://github.com/openziti/llm-gateway) | summary | A gateway alone is not new work. What this project can add is the authenticated path across networks, the placement and the account |
| exo splits one model over home devices, peer to peer | [exo](https://github.com/exo-explore/exo) | summary | The split of one model is not the subject of this record (design 0038, D5) |
| In Vietnam, issuing and trading crypto assets runs under a licensed pilot (Resolution 05/2025/NQ-CP) | [Allen & Gledhill](https://www.allenandgledhill.com/perspectives/articles/31332/vnkh-establishes-digital-technology-framework-and-launches-crypto-asset-market-pilot-program) | summary | One more reason for the default of Q14. This is not legal advice |

## Goals

- **G1** The owner can run the trial before anything is built here, and its records decide what
  is built.
- **G2** A program on one machine talks to a model server on another machine as if the server
  were local. Replies stream. The model server never listens beyond loopback.
- **G3** The master records who served whom without reading model traffic into the record.
- **G4** The account uses the credits of [design 0054](0054-open-compute-network.md), D6. Nothing
  in it can be bought or sold.

## Non-goals

- An inference engine, a model, or the split of one model over machines (design 0038, D5).
- A method that is not a neural network and answers like a language model. This record knows of
  none. What can replace a model for part of the work is ordinary code for fixed commands, and
  that belongs to the application.
- A tradable token (design 0054, Non-goals; Q14).
- Model traffic to the machines of strangers. A worker can read every prompt it serves, and
  LIVA promises that the user's data stays with the user. This record stays inside clusters of
  people who trust each other (Q13, model A).
- Changes to LIVA. KH-1 to KH-3 belong to its repository.
- A change to the text of LLM-4 or LLM-6. Q41 decides the order first.

## Design

### D1 Roles

| Part | OxideSwarm | LIVA | Others |
|---|---|---|---|
| Runs the model | No | A small model in its own process, for when no other machine answers | llama.cpp or Ollama on the machine that holds the model |
| Carries the traffic between machines | Yes: LLM-9 | No | |
| Says who may connect, and encrypts | Yes: the cluster secret, the handshake and AEAD records that exist today; a key per machine with ENROLL-1 | Shows the user when a question leaves the machine (KH-3) | |
| Keeps the account | Yes: LLM-10 and LLM-11 | No | |
| Gives a person a reason to install anything | Not for audience A3 of [design 0059](0059-first-users.md) | Yes | |

### D2 Order

1. **APP-1**, the trial. It uses SSH in place of the grid.
2. The owner reads the trial records and decides Q41.
3. **LLM-9**, the client tunnel.
4. **LLM-10**, usage records. It needs ENROLL-1 to name the client.
5. **LLM-11**, a balance per member.
6. LLM-6 then chooses a worker and uses the tunnel. Credits between clusters stay with NET-2 of
   design 0054.

Steps 3 to 6 are proposals. Only step 1 is decided (Q40).

### D3 The trial (APP-1)

The trial answers one question: is LIVA on a machine without the model good enough to use when
another machine computes the model?

**The stand-in for the grid.** The model server runs on the second machine and listens on
loopback. The Mac opens an SSH local port forward to it. The model server is never exposed, and
the traffic is encrypted. LLM-9 has the same shape, with one more hop through the master.

**Part A: the path, with no change to LIVA.**

1. On the second machine, a model server serves the chat model that LIVA uses, on loopback.
2. On the Mac, an SSH forward maps a loopback port to that server.
3. On the Mac, `liva_eval --provider cloud --api-base http://127.0.0.1:<PORT>/v1` runs LIVA's
   benchmark suite against it.
4. The same suite runs with the model on the Mac itself, where the Mac can hold it.

Whether `liva_eval` builds on macOS at LIVA's `main` is not known. If it does not, a streaming
request sent with `curl` checks the path, and the timing waits for Part B.

**Part B: the application.** It needs KH-1 in LIVA and a build of LIVA that starts on macOS.

1. LIVA on the Mac answers a typed question and a spoken question with the model on the second
   machine.
2. The second machine is stopped in the middle of an answer. What LIVA does is written down.

**What each trial record holds.**

- The date, LIVA's commit, and the model file.
- Both machines: processor, memory, GPU. The network between them: the same local network, or
  not.
- Time to first token and tokens per second as `liva_eval` reports them, for the local and the
  remote run, with the suite file named.
- Every step that failed, and what the person had to do by hand.

Records go into "Trial records" below. No number from the trial is quoted anywhere without its
record.

**A spike on the same two machines.** Spike R14 of
[design 0056](0056-frontier-program.md) (D9) times what comes before the first reply: moving
the model file, loading it, and the first token. It can run in the same sitting as the trial.

### D4 Named model services and a client tunnel (LLM-9)

**On the worker.**

- The owner starts a model server on loopback, as in LLM-1.
- `worker --serve <NAME>=127.0.0.1:<PORT>` names it. The flag can repeat. The worker adds the tag
  `serve:<NAME>`, and `serve:` joins `RESERVED_TAG_PREFIXES` (`crates/core/src/placement.rs`).
- A worker without `--serve` refuses every tunnel. A tunnel reaches only a named port. A client
  names a service and never an address.

**On the client.**

- `oxideswarm llm connect --service <NAME> [--listen 127.0.0.1:<PORT>]` listens on loopback. It
  refuses any other listen address in the first version.
- Each connection it accepts becomes one tunnel.

**Through the master.**

1. The client opens a tunnel connection to the master: the cluster-auth handshake, then a
   request that names the service.
2. The master picks a worker that has the tag and the capability bit, with the fewest open
   tunnels. Placement entries (`require:worker=`, `require:tag=`) narrow the choice as they do
   for tasks.
3. The master asks the worker on its control path. The worker connects to the named loopback
   port. Then it opens a tunnel connection of its own to the master.
4. The master joins the two connections. Bytes pass both ways in bounded chunks. When one side
   closes, the other closes.

- **Transport.** The tunnel uses the connection types that design 0038 D4 names: on TCP, a
  connection type of its own that runs the handshake and then AEAD records; on iroh, a stream
  kind of its own. Model bytes never share the control stream.
- **No retry.** When the worker is lost in the middle of a reply, the client's connection
  closes. The master does not send the request again, because it cannot know whether the request
  is safe to repeat. The application falls back, as LIVA's plan says.
- **Limits.** A cap of open tunnels per worker, set by its owner. A cap on the master. An idle
  time after which a tunnel closes. A cap per client once ENROLL-1 names clients.
- **What it does not do.** It does not parse HTTP, start the model server, or load a model.
  Because it carries bytes, a streamed reply, an audio upload and an image request pass without
  a route written for each.

### D5 Usage records (LLM-10)

- The master writes one record when a tunnel closes: the tunnel id, the service name, the worker
  id, the client's identity, the start and end times, the bytes each way, and why it closed.
- The client's identity is the machine key of ENROLL-1. Before ENROLL-1 the record cannot name
  the client, so LLM-10 needs it.
- A record holds no content of the traffic.
- Records stay on the master, in a bounded file. `oxideswarm llm usage` and the dashboard show
  them. The binaries send them nowhere, which keeps the rule of Q39.
- A tunnel cannot count tokens. The gateway of LLM-6 can add token counts later, where the model
  server reports them in the `usage` object of an OpenAI-compatible reply.

### D6 A balance per member (LLM-11)

- A balance is a view over usage records: what a member's machines served, minus what the
  member's clients used.
- Each record credits one member and debits another by the same amount. No record creates
  credit, which is the rule of design 0054 D6.
- **The unit is open.** Candidates: bytes of reply, seconds of open tunnel, or tokens once the
  gateway counts them. The choice waits for real usage records.
- The first version displays the balance and refuses nobody (Q43).
- Inside one cluster the master's record is the account, because the cluster's members already
  trust its master. Between clusters, both sides sign each record: that is NET-2.

### D7 What LLM-4 and LLM-6 become

This is the proposal that Q41 decides. Until then both keep their text.

- **LLM-4** reuses the tunnel connection of LLM-9 for `ggml-rpc-server`. One mechanism carries
  both.
- **LLM-6** keeps its routes and its queue. Its streaming version uses LLM-9 and no longer waits
  for service tasks (LLM-2), as long as the owner starts the model server by hand.
- **The reason.** A voice assistant has to speak while the reply is still arriving. A first
  version that returns the reply whole does not serve it.

## Compatibility

- **Wire change.** LLM-9 adds a tunnel request, a reply and a refusal, on the client side and on
  the worker side. Each public message enum needs the new variants in both mirror enums and all
  four `From` impls (`crates/core/src/protocol.rs`).
- **A capability bit** in `ProtocolCapabilities`. The master sends tunnel requests only to a
  worker that negotiated it.
- **A protocol version bump.** The master is upgraded first, as for every bump
  (`DEPLOYMENT.md` 3.5).
- **A reserved tag prefix.** A user tag that starts with `serve:` is refused after the change.
- LLM-10 and LLM-11 change no wire format. They add a file on the master and two read-only views.
- APP-1 changes nothing here.

## Security

- **Reach.** The tunnel reaches only the loopback ports that the worker's owner named. Without
  this rule a tunnel would be a way to reach any local service of a worker.
- **The model server needs no network exposure.** It keeps listening on loopback, which is what
  llama.cpp's security policy asks for.
- **The master can read the traffic.** Encryption is per hop: client to master, master to
  worker. Q42 asks whether that is enough. Encryption from end to end needs the machine keys of
  ENROLL-1.
- **The worker's owner can read the traffic.** The model server sees every prompt. An
  application that uses the tunnel has to tell its user so (KH-3 in LIVA).
- **The client listener** is open to every local process of the client machine, as a local model
  server is.
- **Tasks on the same worker.** A task that runs under `--isolation off` or `fs` can reach the
  worker's loopback ports, and so the model server, without a tunnel and without a usage record.
  `fs+net` closes that. The guide for LLM-9 has to say so.
- **Usage records** say who used a service and when. They hold no content, the file is bounded,
  and nothing sends it off the master.
- **Tests** give every child process a temporary `HOME`, as the repository's rules require.

## Test plan

- **Unit.** Refusal without `--serve`, refusal of an unnamed service, the caps, the idle close,
  the reserved prefix.
- **Integration.** A test HTTP server on loopback stands in for the model server and sends a
  reply in parts with pauses. The test asserts that the first part reaches the client before the
  server sends the last one. A second test stops the worker in the middle and asserts that the
  client's connection closes and that one usage record with the right reason exists.
- **Mixed versions.** A worker without the capability bit gets no tunnel request.
- **Live.** Part B of APP-1 runs again with LLM-9 in place of SSH, and the record holds both
  sets of times.

## Work items

"Needs" lists items of this record, of other design records and of LIVA's plan. Sizes are the
backlog's S, M and L. No item is in [BACKLOG.md](../BACKLOG.md).

| ID | Item | Size | Needs | Files | Done when | Status |
|---|---|---|---|---|---|---|
| APP-1 | Trial: LIVA on a Mac, the model on another machine, over an SSH forward | S, owner | Nothing for Part A. KH-1 of LIVA and a build that starts on macOS for Part B | This record | "Trial records" holds a dated entry for Part A and one for Part B | Planned |
| LLM-9 | Named model services (`worker --serve`) and `llm connect` | L | APP-1, Q41. ENROLL-1 before a cluster of several owners uses it (Q42) | `crates/core/src/protocol.rs`, `crates/core/src/placement.rs`, `crates/master/src/`, `crates/worker/src/`, `crates/cli/src/`, `README.md`, `docs/SECURITY.md` | An unchanged OpenAI client on one machine streams a reply from a loopback model server on another, the integration tests of the test plan pass, and a worker without `--serve` refuses | Planned |
| LLM-10 | Usage records for tunnels, `llm usage`, a dashboard view | M | LLM-9, ENROLL-1 | `crates/master/src/`, `crates/cli/src/`, `README.md` | Each closed tunnel leaves one record that names the client's machine key and the worker, and holds no content | Planned |
| LLM-11 | A balance per member, display only | S | LLM-10, Q14, Q43 | `crates/master/src/`, `crates/cli/src/` | The balances of all members sum to zero, and the view shows them | Planned |

## Decisions for the owner

The right column is a recommended default, not a decision, unless it says "Decided". They are
listed in [ROADMAP.md](../ROADMAP.md) as Q40 to Q43.

| ID | Question | Recommended default |
|---|---|---|
| Q40 | Does a trial of LIVA on the Mac, with the model on another machine, come before any build item of this record? | Decided on 2026-11-11: yes, the owner asked for it |
| Q41 | After the trial: build the client tunnel (LLM-9) before the gateway (LLM-6), and let LLM-4 reuse it? | Yes, if the trial records show that the remote model is worth using |
| Q42 | Is encryption per hop enough, so that the master can read model traffic it relays? | Yes while every machine has one owner. End to end, with ENROLL-1, before a cluster of several owners uses LLM-9 |
| Q43 | Does a balance only display, or does it refuse a member whose balance is below zero? | Display only in the first version |

Q14 (credits, money or tokens) stays open in the roadmap. This record follows its default.

## Risks

| Risk | Answer |
|---|---|
| The trial shows that the remote model is not better than the small local one | Then LLM-9 is not built for LIVA. That is what G1 is for |
| LIVA does not start on macOS | Part A still runs. Part B waits, and the record says what failed |
| The record reads "currency" as credits, and the owner meant money | Q14 is the place to say so. LLM-10 is the same in both cases |
| A balance with no one to trade with means nothing | LLM-11 comes last, and only when at least two members serve each other |
| Usage records become a way to watch people | No content, a bounded file, nothing leaves the master, and members can read their own records |
| A gateway already exists elsewhere, so the work repeats it | LLM-9 is the part those gateways do not have here: the authenticated path, also across NAT. LLM-6 is weighed again after the trial |

## Trial records

None yet.

## Status log

- 2026-11-11: Written at the owner's request, from three statements of the same day (see
  Context). The owner set the trial first (Q40). Nothing is built, no item is in the backlog,
  and the texts of LLM-4 and LLM-6 are unchanged.
- 2026-11-11: The owner asked to study how to get a model onto a machine and ready quickly. D3
  now points to spike R14 of [design 0056](0056-frontier-program.md), which times the fetch,
  the load and the first token on the trial's two machines. Training a model stays out of
  scope (design 0038, Non-goals).
- 2026-11-12: [Design 0062](0062-model-weights-into-remote-memory.md) builds on LLM-9. Its
  model server (LLM-12) is a named service, and its client command (LLM-13) sends model
  weights through a tunnel. The text of LLM-9 is unchanged. The spike that D3 names is R14;
  it was numbered R11 on 2026-11-11.
