# E2E Test Infra: rusty_grid

## Test Philosophy
- Opaque-box, requirement-driven. No dependency on implementation internal design.
- Direct validation of Acceptance Criteria:
  1. 1 Master + 3 Workers locally (1 simulated GPU).
  2. All 3 Workers register successfully with Master.
  3. Generic task submitted, executed, and correct result returned.
  4. GPU-specific task submitted and assigned ONLY to simulated GPU worker.
  5. Batch of 5 independent compilation tasks executed in parallel across workers.
- Methodology: Category-Partition + Boundary Value Analysis (BVA) + Pairwise Combinations + Real-World Workloads.

## Feature Inventory & Test Mapping
| # | Feature | Source | Tier 1 (Equivalence) | Tier 2 (Boundaries) | Tier 3 (Pairwise) |
|---|---------|--------|:--------------------:|:-------------------:|:-----------------:|
| 1 | Master-Worker Connection & Reg | ORIGINAL_REQUEST §R1, AC1, AC2 | 5 | 5 | ✓ |
| 2 | Worker Capability Advertisement | ORIGINAL_REQUEST §R1, AC1, AC4 | 5 | 5 | ✓ |
| 3 | Generic Task Execution | ORIGINAL_REQUEST §R2, AC3 | 5 | 5 | ✓ |
| 4 | Workload-Specific GPU Routing | ORIGINAL_REQUEST §R3, AC4 | 5 | 5 | ✓ |
| 5 | Parallel Batch Crate Distribution | ORIGINAL_REQUEST §R3, AC5 | 5 | 5 | ✓ |

## Test Architecture
- **Automated Shell Harness**: `test_integration.sh`
  - Location: `/Volumes/KINGSTON/teamwork_projects/rusty_grid/test_integration.sh`
  - Invocation: `bash test_integration.sh` (or `cargo test` for native tests)
  - Exit code: 0 on success, non-zero on assertion failure.
  - Lifecycle: Spawns Master on ephemeral port (`--listen 127.0.0.1:0 --port-file ...`), spawns Worker 1 (CPU), Worker 2 (CPU), Worker 3 (simulated GPU `--simulate-gpu`). Graceful process group cleanup on EXIT signal (`trap cleanup EXIT INT TERM`).
- **Native Rust E2E Suite**: `tests/e2e_cluster.rs`
  - `ClusterHarness` RAII test fixture ensuring automatic process termination.

## Real-World Application Scenarios (Tier 4)
| # | Scenario | Features Exercised | Complexity |
|---|----------|--------------------|------------|
| 1 | End-to-End Acceptance Test (AC1-AC5) | F1, F2, F3, F4, F5 | High |
| 2 | Multi-Worker Parallel Rust Crate Compilation | F1, F3, F5 | High |
| 3 | Heterogeneous GPU / Generic Workload Interleaving | F1, F2, F3, F4 | Medium |
| 4 | Worker Disconnect and Task Reassignment | F1, F3, F4 | High |
| 5 | Heavy Batch Burst Execution (10+ Tasks) | F1, F3, F5 | High |

## Coverage Thresholds
- Tier 1: ≥5 test cases per feature (25 total)
- Tier 2: ≥5 boundary test cases per feature (25 total)
- Tier 3: ≥10 pairwise combinatorial test cases
- Tier 4: ≥5 realistic application workload scenarios
