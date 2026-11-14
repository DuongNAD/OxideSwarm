# 0057: Charter for donated compute (open results)

Status: Accepted (principle: the owner accepted Q27 to Q29 on 2026-11-10; the charter's text awaits
a legal review; builds nothing)
Created: 2026-11-10    Updated: 2026-11-10

## Summary

The owner proposed a rule on 2026-11-10: research that runs on compute people donate through
OxideSwarm must be free and public. This record turns the rule into a draft charter for the public
pool of [design 0054](0054-open-compute-network.md).

- Work that runs on donated machines publishes its outputs and its task code under open licences.
- The rule is a condition for receiving donated compute. It is not a term of the software licence.
- A cluster that runs on its owner's machines is not covered.
- What people later build from the published outputs is not restricted.
- The charter binds the project's owner as it binds everyone else.

This record is a policy draft. It is not legal text, and a lawyer reviews the charter before it
binds anyone (CHARTER-1). It builds nothing.

## Context

- **The licence.** OxideSwarm is `MIT OR Apache-2.0` (decision Q11 of
  [ROADMAP.md](../ROADMAP.md)). Both licences let anyone use the software for anything.
- **The public pool.** Design 0054 plans a pool where strangers offer machines, from stage H2 on.
  Its D7 names transparency and a steward (Q17). It does not say what a project owes in return for
  the compute it receives.
- **Receipts and a log.** [Design 0056](0056-frontier-program.md) plans a signed receipt per task
  (PROOF-1) and a public log of receipts (PROOF-4). A receipt lists the output blob ids.
- **Campaigns.** The owner may one day call on contributors to give their idle time to one
  research goal. Such a campaign is a project under this charter, like any other.

Precedents, checked on 2026-11-10. "Basis" is used as in design 0056: **page** means the primary
page was read, **summary** means the claim comes from search results.

| Finding | Source | Basis | Consequence |
|---|---|---|---|
| The Human Genome Project's Bermuda Principles of 1996 called for automatic, rapid release of sequence assemblies to the public domain. In April 1997 the NHGRI required its grantees to release assemblies within 24 hours of their generation | [NHGRI](https://www.genome.gov/10506537/reaffirmation-and-extension-of-nhgri-rapid-data-release-policies) | page | A large project stayed public because the rule was set at the start. D3 |
| World Community Grid accepts research that is conducted by public or nonprofit organisations and benefits humanity, and requires that all data its volunteers generate is made freely available to the scientific community | [Proposal page](https://www.worldcommunitygrid.org/research/proposal.s) | page | Volunteer compute with an openness condition has run for years. D2, D4 |
| The Open Source Definition says a licence must not restrict anyone from using the program in a specific field of endeavour | [OSD, clause 6](https://opensource.org/osd) | page | The rule cannot live in the software licence. D5 |
| The United States policy on dual use research of concern, in effect from 2025-05-06, sorts life-science research with biosecurity risk into two categories. The second covers experiments that enhance a pathogen's transmissibility or virulence. An executive order of 2025-05-05 called for the policy's revision | [University summary](https://www.uab.edu/ehs/biosafety/durc-pepp) | summary | Some research must not be published in full. Such work is refused at admission. D4 |

Why now: a rule of this kind is easy to set before anyone gains from secrecy, and hard afterwards.
It is also the reason a person would offer a machine.

## Goals

- **G1** A contributor knows, before offering a machine, that what it computes will be public.
- **G2** Nobody uses donated compute for outputs that stay closed. This includes the owner.
- **G3** A research group can still publish first, and anyone can still build products later.
- **G4** Work that is dangerous to publish never enters the pool.
- **G5** Where possible, a machine checks the rule, not a person.

## Non-goals

- A change to the software licence, or any limit on what the software may be used for.
- Rules for a cluster on its owner's machines, or for clusters that lend machines to each other by
  their own agreement (stage H1), unless that agreement names the charter.
- Legal text. The draft in D7 states intent in plain words.
- Choosing which science deserves compute. Allocation belongs to the steward of Q17. The charter
  only sets the conditions every project meets.

## Design

### D1 Scope and terms

- **Contributor:** a person or organisation that offers a machine to the public pool.
- **Pool task:** a task that runs on a contributor's machine through the public pool (stage H2 and
  later), or on a machine lent under an agreement that names the charter (for example EDU-3 of
  design 0056).
- **Project:** the group that asks for pool tasks. A campaign the owner calls for is a project.
- **Outputs:** the output blobs of pool tasks, and the datasets and results assembled from them.
- **Tools:** the task code, the configuration and the scripts needed to run a pool task again.

A task that is not a pool task is outside the charter.

### D2 What is open

A project publishes:

- **The outputs,** under the licence of Q29, at a public address, without registration and without
  a fee.
- **The tools,** under a licence the Open Source Initiative approves.
- **A description,** before its first pool task: what the project is for, who runs it, who pays
  for it, and where the outputs will appear.
- **Its papers.** A publication that rests on the outputs has a copy anyone can read without
  paying.

A project also makes **no claim on the outputs**: it asserts no patent and no database right
against anyone for using the published outputs.

The charter does **not** cover what is built afterwards. A person or company may use the published
outputs to develop a product, and may protect that product. Without this, nobody would pay for the
trials that turn a finding into a medicine. The outputs themselves stay open to everyone else.

### D3 When

- Outputs are published no later than the embargo of Q28, counted from the end of the batch of
  tasks that produced them.
- Outputs are published at once when a paper that uses them appears, if that is earlier.
- A project whose outputs are checks, such as the rebuild receipts of PROOF-5, publishes at once.
- There is no extension. A project that needs longer does not use the pool.

The embargo exists so that the group that designed the work can analyse it first (G3).
Contributors see the date before they join a project (G1).

### D4 Admission

- **A public application.** It states the purpose, the organisation, the funding, the kind of
  tasks, the publication address, the licences, and a safety statement.
- **Open to anyone.** A public body, a non-profit, a person or a company may apply. The condition
  is on the outputs, not on who asks. World Community Grid admits only public and nonprofit
  organisations; this charter differs, and leaves that choice to each contributor (below).
- **Refused at admission,** not after the work is done:
  - research that enhances the transmissibility or the virulence of a pathogen, or designs toxins
    or weapons;
  - any work whose outputs could not be published safely;
  - work on personal data of people who did not agree to it;
  - the task classes design 0054 D7 already refuses: mining, scanning and attacks.
- **Institutional review.** Where the applicant's institution or country reviews such research,
  the application states the outcome of that review.
- **Reviewers.** Until a steward exists (Q17), the owner and at least two reviewers from outside
  the project decide. Each decision is published with its reason. A project of the owner goes
  through the same review, and the owner does not vote on it.
- **Contributors choose.** A contributor picks the projects a machine serves (NET-4 of design
  0054). No project receives a machine without its contributor's consent. A call from the owner is
  an invitation, and each contributor answers it alone.

### D5 Enforcement

The software licence cannot carry the rule (Context). Access to the pool carries it:

- **Commitment.** A project signs one version of the charter, named by its digest, with the
  project's key. The signed commitment is public.
- **Receipts.** The receipt of every pool task (PROOF-1) names the project and the charter digest.
  A pool worker refuses a pool task without them. The log (PROOF-4) then shows which project used
  how much.
- **A check a machine runs.** Receipts list output blob ids. A project publishes a manifest of its
  outputs with the same ids. `oxideswarm charter check <project>` hashes the published files,
  compares them with the log, and lists every output that is past its embargo and not published.
  Anyone can run it.
- **Consequences.** A project with a late output receives no new pool tasks until it has
  published. A project that refuses loses its key in the pool, and the log records why.
- **Escrow (to be designed in CHARTER-3).** Where sizes allow, the pool keeps the output blobs and
  publishes them itself when the embargo ends. The rule then holds without the project's help.

### D6 Changing the charter

- The charter has versions, and each version has a digest. A project is bound by the version it
  signed for the outputs it produced under it.
- No change removes openness from outputs that already exist.
- Before a steward exists, a change may add openness and may not remove any. The owner cannot
  loosen the charter alone.
- With a steward, a change needs the steward's decision after public notice.
- A contributor may leave at any moment, with the local stop switch of NET-4.

### D7 The charter (draft text)

This is the text a contributor reads and a project signs. It is a draft in plain words.

```text
Charter for donated compute, draft 0

1. This charter applies to work that runs on machines people offer to the OxideSwarm public pool.
   It does not apply to anyone's own machines.
2. A project that receives donated compute publishes what that compute produced: the outputs, and
   the code needed to produce them again.
3. The outputs are free to read, copy and use, under an open licence, without registration and
   without a fee.
4. The project publishes them no later than the fixed delay after the work ends, and at once when
   it publishes a paper that uses them.
5. The project claims no patent and no database right against anyone for using the outputs.
6. Anyone may build on the published outputs, and may protect what they build. Nobody may close
   the outputs themselves.
7. Before it starts, the project says in public what it is for, who runs it and who pays for it.
8. Work that would be dangerous to publish is not admitted. Work that harms people is not
   admitted.
9. Each contributor chooses which projects a machine serves, and may stop at any moment.
10. A project that breaks this charter receives no more donated compute, and the public log
    records it.
11. This charter binds the people who run OxideSwarm in the same way. They cannot weaken it alone.
```

## Compatibility

Nothing changes before stage H2. Private clusters never see the charter. Q27 was accepted before
PROOF-1 is built, so the receipt has two fields from the start: the project and the charter digest.
That avoids a second change to the receipt encoding.

## Security

| Exposure | Rule |
|---|---|
| A project applies under a false purpose | The description and the funding are public before the first task. Contributors choose per project. Reviewers publish their reasons |
| A project publishes late or not at all | `charter check`, the stop on new tasks, and escrow where sizes allow |
| A project splits its work into endless batches to delay the embargo | The embargo counts per batch, and a batch has a maximum length set in CHARTER-2 |
| Openness itself does harm | Refusal at admission (D4). The pool never holds outputs that it would be dangerous to publish |
| Outputs contain personal data | Refused at admission. Pool inputs are visible to contributors in any case (design 0054, non-goals) |
| The owner is pressed to grant an exception | D6: no version removes openness before a steward exists |
| Reviewers are friends of the applicant | Reviewers are named, and a reviewer with a link to the project does not vote |

## Test plan

Nothing is testable until the items below are built. Then:

- A receipt of a pool task without a project or a charter digest is refused by a pool worker.
- `charter check` on a fixture reports an output past its embargo that the manifest lacks, and
  passes when the manifest and the files match the log.
- A project whose key was removed cannot submit a pool task.
- The charter's digest is stable: a golden test fixes the bytes of each published version.

## Work items

| ID | Item | Needs | Files | Done when | Status |
|---|---|---|---|---|---|
| CHARTER-1 | The owner's decisions on Q27 to Q29, and a legal review of the text in D7 | | this record | The decisions are in `ROADMAP.md`, and the review and its changes are in this record's status log | Partial: decided on 2026-11-10, review open |
| CHARTER-2 | Application form, review checklist with the safety screen, maximum batch length, format of a published decision | CHARTER-1 | `docs/guides/` | A sample application passes through the checklist, and its decision is written in the published format | Planned |
| CHARTER-3 | Project and charter digest in receipts, refusal by pool workers, design of escrow | CHARTER-1, PROOF-1, NET-4 | the files of PROOF-1 | The first two tests of the test plan pass | Planned |
| CHARTER-4 | `oxideswarm charter check` | CHARTER-3, PROOF-4 | `crates/cli/src/` | The `charter check` test of the test plan passes | Planned |
| CHARTER-5 | Change procedure under the steward | Q17, NET-7 | to be designed | A change to the charter follows the written procedure once, with its public notice | Planned |

No item starts before stage H2 is planned in detail, except CHARTER-1, which only needs the
owner's word.

## Decisions for the owner

The owner accepted all three on 2026-11-10, with the defaults this record recommended.

| ID | Question | Decision |
|---|---|---|
| Q27 | Accept the charter's principle: outputs and tools of donated compute are open, later products are free, private clusters are not covered, and the charter binds the owner? | Yes |
| Q28 | How long is the embargo? | At most six months after a batch ends, and none for outputs that are checks |
| Q29 | Which licences? | CC BY 4.0 for outputs by default, CC0 allowed. Any licence the Open Source Initiative approves for tools |

## Risks

| Risk | Mitigation |
|---|---|
| Good research groups stay away because of the openness rule | The embargo of D3, and freedom for later products (D2) |
| The rule exists and nobody enforces it | The check a machine runs (D5), receipts and the log |
| Patent and database law differ by country, and the plain text leaves gaps | The legal review of CHARTER-1, before stage H2 |
| Published outputs are open and unusable | The tools and a description are part of what is published (D2) |
| The charter is weakened later | D6 |
| The charter is written long before a pool exists and goes stale | It is short, versioned, and reviewed again when stage H2 is planned |

## Status log

- 2026-11-10: Draft, from the owner's proposal that research done with compute the project
  mobilises is free and public. Precedents checked on the web the same day. Decisions Q27 to Q29
  are open. Nothing is built.
- 2026-11-10: The owner accepted Q27 to Q29 with the recommended defaults. Status set to Accepted
  for the principle. The text of D7 stays a draft until the legal review of CHARTER-1.
