# Concept Mapping

This document maps the core concepts taught in DS614: Big Data Engineering to what was built, observed, and analyzed in this project on Apache Airflow's retry execution behavior.

---

# 1. Data Orchestration

## The Concept

In class, we studied why naive pipeline setups — cron jobs chaining Python scripts — fall apart in practice. If `transform.py` fails, the entire pipeline stops. There is no retry logic, no visibility, and no partial rerun support. The orchestration lecture defined the solution as an automated way of handling data workflows where tasks execute based on schedules, dependencies, and system state — with automated handling of failures and safe re-execution.

The seven components taught were: Workflow Definition (DAGs), Scheduling and Triggering, Execution Engine, State Management, Fault Tolerance and Retry Handling, Observability and Monitoring, and Re-execution and Backfill Support.

## Where We Used It

This entire project is a direct study of those seven components inside a real orchestration system. We defined workflows as DAGs, triggered them through the Scheduler, observed state transitions in the Airflow UI, and modified the retry logic inside `taskinstance.py` — the source file responsible for fault tolerance and re-execution.

## How It Helped

Understanding orchestration as a concept gave us the right mental model before opening any source code. When we saw the state machine `RUNNING → FAILED → UP_FOR_RETRY → RUNNING`, we recognized it immediately as Airflow's implementation of the fault tolerance and state management components described in class — rather than treating it as an arbitrary implementation detail.

---
# 2. Fault Tolerance and Partial Failure

## The Concept

The Distributed Systems Challenges lecture established the most important mindset shift in the course: in distributed systems, partial failure is the normal case, not the exception. The lecture formalized this with a key contrast:

| Aspect | Single-Node | Distributed |
|---|---|---|
| Error Handling | Exceptions are exceptional | Failures are expected; retries are common |
| Retry Semantics | Usually unnecessary | Must consider idempotency and duplicate effects |

It also introduced Safety and Liveness as the two correctness properties a distributed system must uphold. Safety means nothing bad happens — violations are permanent. Liveness means something good eventually happens — the system makes progress.

## Where We Used It

The retry mechanism in Airflow is a direct software implementation of these ideas.

**Experiment 1 — Exponential Backoff:** The lecture warned that immediately retrying a failed operation adds load to an already-struggling system. We implemented exponential backoff in `next_retry_datetime()` as the principled response:

```python
delay_seconds = delay_seconds * (2 ** (self.try_number - 1))
```

Attempt 1 → base delay. Attempt 2 → 2× delay. Attempt 3 → 4× delay. This prevents retry storms.

**Experiment 3 — Failure-Aware Retry:** The lecture noted that a timeout cannot distinguish between a crashed node and a slow one. We applied the same idea to exception types — not all failures deserve the same response:
AirflowException      → transient failure (network) → RETRY
AirflowFailException  → permanent failure (logic)   → FAIL IMMEDIATELY

Retrying a logic error wastes all remaining attempts. Retrying a network error is correct.

**Safety and Liveness in our DAGs:** Downstream tasks are blocked until upstream recovery succeeds (Safety). The retry loop guarantees a recoverable failure will eventually complete (Liveness).

## How It Helped

The fault tolerance lecture reframed how we read Airflow's source code. `handle_failure_context()` is not just an error handler — it is the system's failure model expressed in code. Every parameter we configured (`retries`, `retry_delay`, exception type) is a tunable knob in a fault tolerance strategy, not just a setting.

---

# 3. Batch Processing

## The Concept

The batch processing lecture drew a clear line between three system types. Services are online — a user is waiting, so latency is the metric. Batch systems are offline — no user is waiting, jobs run periodically, and the metric is throughput: how much data gets processed reliably in a given window. The lecture also showed how even a simple Unix pipeline follows the same decomposition principle as MapReduce — break a large task into a sequence of ordered, isolated transformations where each stage feeds the next.

## Where We Used It

Airflow is a batch workflow orchestration system, and this project treats it as one. Our experiments simulate stages of a data pipeline — tasks that must execute in order, where failures in early stages must be handled before later stages can proceed. The execution model we traced:
Task Submission → Scheduler → Worker Execution → Retry Handling → Final State

This is the same decomposition philosophy taught in class — each stage is isolated, ordered, and recoverable.

## How It Helped

Framing Airflow as a batch system helped us understand why throughput and reliability matter more than response time here. The point of retry logic is not to respond fast — it is to ensure the job eventually completes correctly. That shift in metric gave us the right lens for evaluating whether our retry experiments were actually successful.

---

# 4. B-Tree Based Metadata Storage

## The Concept

The B-Trees lecture explained a fundamental constraint: disk is block-addressable, not byte-addressable. A disk seek is roughly 100,000× slower than a RAM access, so the number of disk seeks dominates performance. Binary trees are tall and thin — searching 1 billion records takes ~30 disk seeks. B-Trees are wide and shallow — the same search takes only ~3 disk seeks. This is why every major relational database (SQLite, PostgreSQL) uses B-Tree indexes internally.

## Where We Used It

Airflow stores all workflow state in a relational database — `airflow.db` (SQLite) for Experiments 1–3 and PostgreSQL for Experiment 4. Every piece of information the Scheduler needs to manage retries lives here:
try_number    — how many attempts have been made
state         — current task state (FAILED, UP_FOR_RETRY, SUCCESS)
start_date    — when the task started
end_date      — when it ended (used to compute the next retry time)

On every scheduling cycle, the Scheduler queries this database to find failed tasks, check retry limits, and compute the next retry time. These queries are fast because of B-Tree indexes on fields like `task_id`, `dag_id`, and `state`.

Experiment 4 made this concrete — switching to LocalExecutor caused multiple subprocesses to write the metadata database simultaneously. SQLite's single-writer B-Tree model collapsed with `database is locked` errors. Switching to PostgreSQL, which supports concurrent access with row-level locking, fixed it.

## How It Helped

The B-Tree lecture gave us the vocabulary to explain why the metadata database is not an afterthought — it is the backbone of the retry mechanism. It also connected two things that seemed unrelated: executor type and database choice. The storage model taught in class explained exactly why LocalExecutor broke SQLite and why PostgreSQL was the correct fix.

---



# 5. Scheduler as a Coordination Component

## The Concept

The Distributed Systems Challenges lecture discussed how coordination in distributed systems is hard because nodes cannot share memory. A coordinator must infer system state by reading messages — which can be delayed, lost, or stale. This is why explicit state management and persistent state logs are standard patterns: they give the coordinator a reliable, queryable view of the system without requiring direct communication between every component.

## Where We Used It

Airflow's Scheduler is the centralized coordination component for the entire workflow. It does not share memory with the Workers executing tasks. It coordinates entirely through the metadata database — Workers write their state; the Scheduler reads it, makes decisions, and writes scheduling instructions back.

The retry coordination loop the Scheduler runs on every cycle:

1. Query metadata database for tasks in `FAILED` state
2. Check `try_number` against the `retries` limit
3. Compute next retry time from `end_date + retry_delay`
4. Update task state to `UP_FOR_RETRY`
5. Re-dispatch task to executor after delay

Modifying `fetch_handle_failure_context()` in Experiment 2 made this visible at the source code level. The function reads task state, applies the priority-based delay calculation, and writes updated state back — all through database-mediated coordination, with no direct communication between Scheduler and Worker.

## How It Helped

Seeing the Scheduler as a coordinator rather than just a cron replacement reframed the entire project. Every retry we observed was not just "the task running again" — it was the output of a coordination protocol between components, mediated by persistent state in a B-Tree-indexed database, designed to tolerate partial failure. The coordination, storage, and fault tolerance concepts from class all came together in a single code path.

