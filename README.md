# Airflow Execution Retries

A systems-level analysis project on Apache Airflow focused on workflow orchestration, task execution retries, scheduler behavior, fault tolerance, and pipeline reliability.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Project Objective](#project-objective)
- [Project Structure](#project-structure)
- [System Architecture](#system-architecture)
- [Experiments](#experiments)
  - [Experiment 1 — Airflow Task Lifecycle and Retry Behavior](#experiment-1--airflow-task-lifecycle-and-retry-behavior)
  - [Experiment 2 — Priority-Based Retry](#experiment-2--priority-based-retry)
  - [Experiment 3 — Failure-Aware Retry](#experiment-3--failure-aware-retry)
  - [Experiment 4 — Sequential vs Parallel Execution](#experiment-4--sequential-vs-parallel-execution)
- [Configuration Changes](#configuration-changes)
- [How to Run the Project](#how-to-run-the-project)
- [Technologies Used](#technologies-used)
- [Key Concepts Covered](#key-concepts-covered)
- [Learning Outcomes](#learning-outcomes)
- [Contributors](#-contributors)

---

## Project Overview

Apache Airflow is an open-source workflow orchestration platform designed to programmatically author, schedule, and monitor complex data pipelines. It was created to solve a fundamental problem in distributed data engineering: the need to reliably coordinate sequences of dependent tasks across heterogeneous systems, at scale, with full observability and fault tolerance.

In traditional data engineering, pipelines are often implemented as cron jobs, shell scripts, or ad-hoc scheduling mechanisms. These approaches break down quickly under real-world conditions — they lack dependency management, offer no retry logic, provide minimal visibility into failures, and cannot scale across distributed infrastructure. Airflow was built to replace this fragility with a principled, systems-level approach to workflow management.

Internally, Airflow is composed of several tightly coordinated components:

- **DAG Parser** — Continuously scans the DAG directory, parses Python files, and registers workflow definitions into the metadata store.
- **Scheduler** — The central coordination engine. It evaluates DAG schedules and task dependencies, transitions tasks through their lifecycle states, and dispatches ready tasks to an Executor.
- **Executor** — The component responsible for physically running tasks. The choice of Executor determines whether tasks run sequentially in a single thread, in parallel subprocesses on a single machine, or distributed across a cluster of workers.
- **Workers** — Processes (or remote agents, depending on the Executor) that receive dispatched tasks and carry out their execution logic.
- **Metadata Database** — A relational database (SQLite for development, PostgreSQL or MySQL for production) that stores DAG definitions, task states, execution history, retry counts, and scheduling metadata. It is the source of truth for the entire system.
- **Webserver** — A Flask-based UI that provides real-time visibility into DAG runs, task statuses, logs, and execution timelines.

This project studies two of the most critical and operationally significant subsystems within Airflow: **Executors** and **Retry Mechanisms**. These two components sit at the intersection of performance and reliability — they determine how fast work gets done and how gracefully the system recovers when things go wrong.

---

### Executors: The Execution Layer

The Executor is Airflow's task dispatch and execution engine. It is one of the most architecturally consequential configuration decisions in any Airflow deployment, directly determining throughput, parallelism, and scalability.

- **SequentialExecutor** runs one task at a time in a single thread within the Scheduler process. It is deterministic and easy to debug, but fundamentally incapable of concurrent task execution. It is incompatible with production workloads.
- **LocalExecutor** spawns a separate OS-level subprocess for each task, enabling true parallelism on a single machine. It requires a concurrent-safe metadata database backend (PostgreSQL or MySQL) since multiple processes write task state simultaneously. This makes SQLite — a single-writer, file-locked database — architecturally incompatible with LocalExecutor.
- **CeleryExecutor** and **KubernetesExecutor** extend this model to distributed worker pools and container-native environments, respectively, enabling horizontal scalability across clusters.

---

### Retry Mechanisms: Fault Tolerance in Distributed Workflows

In distributed systems, failures are not exceptional events — they are expected. Network timeouts, transient API errors, resource contention, and infrastructure instability are routine operational realities. A workflow orchestration system that treats every failure as terminal is not production-grade.

Airflow addresses this through a configurable retry subsystem built into the `TaskInstance` model. When a task fails, the Scheduler evaluates retry eligibility based on the task's `retries` count and `retry_delay` configuration, transitions the task to `UP_FOR_RETRY`, and reschedules execution after the specified delay. This cycle repeats until the task succeeds or exhausts its retry budget, at which point it transitions to a terminal `FAILED` state.

---

## Project Objective

The goal of this project is not simply to *use* Apache Airflow, but to conduct a **systems-level reverse-engineering analysis** of its internal execution and fault-tolerance architecture — with a focused emphasis on Executors and Retry Mechanisms.

Specifically, the project investigates:

**Executor Architecture and Task Execution:**
- How Airflow dispatches tasks through different Executor backends
- The architectural constraints that govern Executor selection (e.g., database compatibility, process isolation, concurrency models)
- How `SequentialExecutor` and `LocalExecutor` differ in their execution models, throughput characteristics, and infrastructure requirements
- Why parallel task execution necessitates a concurrent-safe metadata database and why SQLite is structurally incompatible with `LocalExecutor`

**Retry Mechanism Design and Fault Tolerance:**
- How Airflow's retry subsystem is implemented within the `TaskInstance` class
- How the `next_retry_datetime()` function governs retry timing and how it can be modified to implement custom backoff strategies
- How `fetch_handle_failure_context()` serves as the central failure state machine and how priority-aware logic can be embedded at the point of state transition

---

## Project Structure

```
Airflow-Execution-Retries/
│
├── airflow_home/                       # Airflow runtime environment
│   ├── dags/                           # DAG files used by Airflow
│   ├── logs/                           # Airflow execution logs
│   ├── airflow.cfg                     # Airflow configuration file
│   ├── airflow.db                      # SQLite metadata database
│   ├── airflow-webserver.pid           # Webserver process ID
│   ├── standalone_admin_password.txt   # Auto-generated admin password
│   └── webserver_config.py             # Webserver configuration
│
├── airflow_src/                        # Modified Airflow source code
│   └── models/
│       └── taskinstance.py             # Modified retry logic implementation
│
├── analysis/                           # Documentation and analysis reports
│   ├── concept_mapping.md
│   ├── design_decision.md
│   ├── execution.md
│   └── failure_analysis.md
│
├── experiments/                        # Experimental DAG implementations
│   ├── exp1_default.py
│   ├── exp_failure_aware_retry.py
│   ├── exp_priority_retry.py
│   └── exp_sequential_vs_parallel_execution.py
│
├── results/                            # Experiment outputs and logs
│   ├── experiment_logs/
│   ├── exp1_task_behavior.txt
│   ├── exp2_priority_based_retry.txt
│   ├── exp3_failure_aware_retry.txt
│   └── exp4_sequential_vs_parallel_execution.txt
│
├── venv/
├── .gitignore
├── README.md
├── requirements.txt
```

---

## System Architecture

Airflow's internal execution path traced in this project:

```
DAG Parsing
    ↓
Scheduler Creates DagRun
    ↓
TaskInstance Creation
    ↓
Executor Dispatch
    ↓
Task Execution
    ↓
Failure Handling
    ↓
Retry Scheduling
    ↓
Final Success / Failure
```

---

## Experiments

---

### Experiment 1 — Airflow Task Lifecycle and Retry Behavior

#### Purpose
Demonstrate Apache Airflow's task lifecycle and understand how the retry mechanism works when a task fails during execution.

#### What Was Observed
- Airflow task state transitions:

  ```text
  RUNNING → FAILED → UP_FOR_RETRY → RUNNING
  ```

- Automatic retry handling by Airflow
- How failed tasks are rescheduled
- The role of `retries` and `retry_delay`
- Changes in `try_number` during execution
- Scheduler behavior during retries

#### Retry Behavior Observed

```text
Try 1 → FAILED
Try 2 → FAILED
Try 3 → SUCCESS (or final FAILED)
```

**DAG file:** `airflow_home/dags/exp1_default.py`

---

#### Implementation Changes

**Source file modified:** `airflow_src/models/taskinstance.py`

##### Modified Function: `next_retry_datetime()`

**Class:** `TaskInstance`

**What it does:** Computes the datetime when a failed task should next be retried.

**Why it was changed:** The default Airflow behavior applies a flat, fixed retry delay. We replaced this with deterministic exponential backoff, where the delay doubles with each retry attempt.

**Modified code:**

```python
def next_retry_datetime(self):
    """
    Custom retry logic: deterministic exponential backoff
    """
    delay_seconds = self.task.retry_delay.total_seconds()

    # Exponential backoff: delay doubles with each retry attempt
    delay_seconds = delay_seconds * (2 ** (self.try_number - 1))

    delay = timedelta(seconds=delay_seconds)
    base_time = self.end_date or timezone.utcnow()

    return base_time + delay
```

**Key change:** `delay_seconds * (2 ** (self.try_number - 1))` replaces a simple fixed multiplier. On attempt 1 the delay is unchanged, on attempt 2 it doubles, on attempt 3 it quadruples, and so on.

---

### Experiment 2 — Priority-Based Retry

#### Purpose
Implement a custom priority-based retry mechanism where a task's `priority_weight` determines how quickly it is retried.

#### Priority levels and their behavior

| Priority Weight | Level  | Retry Delay Multiplier | Effect          |
|-----------------|--------|------------------------|-----------------|
| ≥ 10            | HIGH   | 0.5×                   | Faster retry    |
| 5 – 9           | MEDIUM | 1×                     | Normal retry    |
| < 5             | LOW    | 2×                     | Slower retry    |

#### What Was Observed
- High-priority tasks recover much faster after failure
- Low-priority tasks wait longer between retries, freeing resources for critical tasks
- `priority_weight` is a native Airflow field, requiring no schema changes

**DAG file:** `airflow_home/dags/exp_priority_retry.py`

---

#### Implementation Changes

**Source file modified:** `airflow_src/models/taskinstance.py`

Two functions were modified for this experiment: `next_retry_datetime()` computes the *when* of the next retry, while `fetch_handle_failure_context()` is the central failure handler that determines task state and retry eligibility. Both needed to reflect priority-based delay to ensure consistent behavior.

##### Modified `next_retry_datetime()`

```python
def next_retry_datetime(self):
    """
    Custom priority-based retry logic
    """
    base_delay = self.task.retry_delay.total_seconds()

    # Get priority_weight (default = 1 if not set)
    priority = getattr(self.task, "priority_weight", 1)

    if priority >= 10:
        multiplier = 0.5    # HIGH priority → faster retry
        level = "HIGH"
    elif priority >= 5:
        multiplier = 1      # MEDIUM priority → normal delay
        level = "MEDIUM"
    else:
        multiplier = 2      # LOW priority → slower retry
        level = "LOW"

    delay_seconds = base_delay * multiplier

    self.log.info(
        f"[PRIORITY RETRY ACTIVE] Task={self.task_id} | "
        f"Priority={priority} ({level}) | Delay={delay_seconds}s"
    )

    delay = timedelta(seconds=delay_seconds)
    base_time = self.end_date or timezone.utcnow()

    return base_time + delay
```

##### Modified `fetch_handle_failure_context()`

The failure handler was extended to apply the same priority-based delay at the moment of state transition (when the task moves to `UP_FOR_RETRY`), ensuring the delay baked into the task matches the one computed in `next_retry_datetime()`.

Key addition inside the `else` branch (task is eligible for retry):

```python
base_delay = ti.task.retry_delay.total_seconds()
priority = getattr(ti.task, "priority_weight", 1)

if priority >= 10:
    multiplier = 0.5
    level = "HIGH"
elif priority >= 5:
    multiplier = 1
    level = "MEDIUM"
else:
    multiplier = 2
    level = "LOW"

delay_seconds = base_delay * multiplier

ti.log.info(
    f"[PRIORITY RETRY ACTIVE] Task={ti.task_id} | "
    f"Priority={priority} ({level}) | Delay={delay_seconds}s"
)

ti.task.retry_delay = timedelta(seconds=delay_seconds)
```

**Why two functions were changed:** Without updating `fetch_handle_failure_context()`, the task's stored `retry_delay` would remain at its original value, causing a mismatch between the logged delay and the actual scheduling time.

---

### Experiment 3 — Failure-Aware Retry

#### Purpose
Implement intelligent retry logic where transient failures (such as network-related issues) are retried automatically, while permanent logic errors fail immediately without consuming unnecessary retry attempts.

#### What Was Observed
- `AirflowException` triggers Airflow retry behavior
- `AirflowFailException` forces immediate final failure
- Retry logic can be dynamically controlled using exception types
- Unnecessary retries for permanent failures were avoided

#### Logic Error Behavior (No Retry)

Observed logs:

```text
RETRY DECISION: Logic error → NO RETRY
Immediate failure requested. Marking task as FAILED
```

Behavior observed:

```text
Attempt 1 → FAILED
No retry triggered
```

#### Network Error Behavior (Retry Enabled)

Observed logs:

```text
RETRY DECISION: Network error → RETRY
Marking task as UP_FOR_RETRY
```

Observed retry timings:

```text
Attempt 1 → 13:39:54
Attempt 2 → 13:40:07 (~13 sec delay)
Attempt 3 → 13:40:19 (~12 sec delay)
```

Behavior observed:

```text
Total attempts = 3
(1 initial attempt + 2 retries)
```

**DAG file:** `airflow_home/dags/exp_failure_aware_retry.py`

---

#### Implementation Changes

**No core source code modifications were made for this experiment.**

This experiment was implemented entirely using DAG-level logic and configuration. The failure-aware behavior was achieved by raising different Airflow exception types directly within the task function:

---

### Experiment 4 — Sequential vs Parallel Execution

#### Purpose
Observe and compare task execution behavior under `SequentialExecutor` vs `LocalExecutor`.

#### What Was Observed
- `SequentialExecutor` runs one task at a time — simple but a bottleneck under load
- `LocalExecutor` spawns separate processes per task — true parallelism within a single machine
- SQLite cannot handle concurrent writes, making it incompatible with `LocalExecutor`

**DAG file:** `airflow_home/dags/exp_sequential_vs_parallel_execution.py`

---

#### Implementation Changes

No Airflow source code modifications were made for this experiment. The behavioral difference was produced entirely through configuration changes to `airflow_home/airflow.cfg`. Details of those changes are covered in the [Configuration Changes](#configuration-changes) section below.

---

## Configuration Changes

### File: `airflow_home/airflow.cfg`

Two settings were changed for Experiment 4.

---

#### 1. Executor

```ini
# Before
executor = SequentialExecutor

# After
executor = LocalExecutor
```

**Why:** `SequentialExecutor` processes one task at a time within a single thread — fine for development and simple experiments but fundamentally incapable of parallel task execution. `LocalExecutor` spawns a separate subprocess per task, enabling true parallelism on a single machine. Switching to `LocalExecutor` was necessary to meaningfully compare sequential vs parallel execution behavior.

---

#### 2. Database Connection

```ini
# Before
sql_alchemy_conn = sqlite:////home/dell/Airflow-Execution-Retries/airflow_home/airflow.db

# After
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost/airflow
```

**Why:** SQLite is a file-based, single-writer database. When `LocalExecutor` spawns multiple parallel task processes, each process attempts to write to the metadata database simultaneously. SQLite cannot handle concurrent writes and will raise `database is locked` errors, causing task failures unrelated to the experiment logic. PostgreSQL supports true concurrent connections and row-level locking, making it the correct backend for any Airflow setup running more than one task at a time. This is also why Airflow's official documentation recommends PostgreSQL (or MySQL) for any production or parallelism-focused setup.

---

## How to Run the Project

### Prerequisites

- Python 3.8+
- Apache Airflow 2.x
- PostgreSQL (for Experiment 4 / parallel execution)
- pip

---

### Step 1 — Clone the Repository

```bash
git clone https://github.com/your-username/Airflow-Execution-Retries.git
cd Airflow-Execution-Retries
```

---

### Step 2 — Create and Activate a Virtual Environment

```bash
python -m venv venv
source venv/bin/activate        # On Windows: venv\Scripts\activate
```

---

### Step 3 — Install Dependencies

```bash
pip install -r requirements.txt
```

If `requirements.txt` is not present, install manually:

```bash
pip install apache-airflow psycopg2-binary
```

---

### Step 4 — Set the Airflow Home Directory

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
```

---

### Step 5 — Set Up the Database

**For Experiments 1–3 (SQLite, default):**

```bash
airflow db init
```

**For Experiment 4 (PostgreSQL):**

First, create the PostgreSQL database and user:

```sql
-- Run inside psql
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
```

Then update `airflow_home/airflow.cfg`:

```ini
executor = LocalExecutor
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost/airflow
```

Then initialise:

```bash
airflow db init
```

---

### Step 6 — Create an Admin User

```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

---

### Step 7 — Apply Source Code Modifications

For Experiments 1 and 2, the modified `taskinstance.py` must replace the installed Airflow source:

```bash
# Find your Airflow installation path
python -c "import airflow; print(airflow.__file__)"

# Copy the modified file (adjust path as needed)
cp airflow_src/models/taskinstance.py \
   /path/to/site-packages/airflow/models/taskinstance.py
```

> **Note:** If using a virtual environment, the path is typically:
> `venv/lib/python3.x/site-packages/airflow/models/taskinstance.py`

---

### Step 8 — Start the Airflow Scheduler and Webserver

Open two separate terminals:

**Terminal 1 — Scheduler:**
```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow scheduler
```

**Terminal 2 — Webserver:**
```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow webserver --port 8080
```

---

### Step 9 — Access the Airflow UI

Open your browser and go to:

```
http://localhost:8080
```

Login with:
- **Username:** `admin`
- **Password:** `admin`

---

### Step 10 — Trigger the DAGs

In the Airflow UI, navigate to the DAGs list and trigger each experiment DAG manually:

| DAG ID                                 | Experiment                            |
|----------------------------------------|---------------------------------------|
| `exp1_default`                         | Experiment 1 — Exponential Backoff    |
| `exp_priority_retry`                   | Experiment 2 — Priority Retry         |
| `exp_failure_smart_retry`              | Experiment 3 — Failure-Aware Retry    |
| `exp_sequential_vs_parallel_execution` | Experiment 4 — Sequential vs Parallel |

You can also trigger via CLI:

```bash
airflow dags trigger exp_failure_smart_retry
```

---

### Step 11 — View Logs

Logs for each task run are available in the Airflow UI under:

```
DAG → Task → Log
```

Or directly in the filesystem:

```bash
ls airflow_home/logs/
```

---

## Technologies Used

| Technology        | Purpose                                      |
|-------------------|----------------------------------------------|
| Apache Airflow 2.x | Workflow orchestration engine               |
| Python 3.8+       | DAG authoring and task logic                 |
| SQLite            | Default metadata DB for Experiments 1–3     |
| PostgreSQL        | Concurrent-safe metadata DB for Experiment 4 |
| LocalExecutor     | Parallel task execution on a single machine  |
| psycopg2          | Python PostgreSQL database adapter           |
| VS Code           | Development environment                      |

---

## Key Concepts Covered

- DAG-based execution models
- Workflow orchestration and dependency resolution
- Fault tolerance and retry mechanisms
- Scheduler-based coordination
- Metadata storage and state management
- Batch pipeline orchestration
- Executor types and their tradeoffs
- Task lifecycle state transitions (`QUEUED → RUNNING → FAILED → UP_FOR_RETRY → SUCCESS`)

---

## Learning Outcomes

Through this project, the following were understood in depth:

- Internal Airflow execution flow from DAG parsing to task completion
- How `TaskInstance` manages retry state and delay calculation
- The difference between `AirflowException` (retryable) and `AirflowFailException` (fail-fast)
- Why `SequentialExecutor` + SQLite cannot support parallelism
- Why PostgreSQL is necessary for production Airflow deployments
- How `priority_weight` can be leveraged beyond UI display to influence scheduling behavior
- Design tradeoffs in retry-driven fault tolerance systems

---

## Detailed Analysis Files

Further analysis can be found in the following documents:

- `analysis/concept_mapping.md` — Maps Airflow internals to distributed systems concepts
- `analysis/design_decision.md` — Explains key design decisions and tradeoffs
- `analysis/execution.md` — Traces the full execution flow
- `analysis/failure_analysis.md` — Studies system behavior under failure conditions

---

## Conclusion

This project conducted a systems-level analysis of Apache Airflow with a focused emphasis on its two most operationally critical subsystems: **Executors** and **Retry Mechanisms**. By reverse-engineering Airflow's internal architecture, modifying core source code, and observing behavioral outcomes under controlled experimental conditions, the project developed a rigorous understanding of how a production-grade workflow orchestration system achieves parallelism, fault tolerance, and reliability at scale.

---

## 👥 Contributors

- Angel Manoj
- Deep Patel

---

*© 2026 –  Airflow-Execution-Retries | The Query Crew*
