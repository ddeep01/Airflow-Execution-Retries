# Airflow Execution Retries

A systems-level analysis project on Apache Airflow focused on workflow orchestration, task execution retries, scheduler behavior, fault tolerance, and pipeline reliability.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Project Objective](#project-objective)
- [Project Structure](#project-structure)
- [System Architecture](#system-architecture)
- [Experiments](#experiments)
  - [Experiment 1 — Exponential Backoff Retry](#experiment-1--exponential-backoff-retry)
  - [Experiment 2 — Priority-Based Retry](#experiment-2--priority-based-retry)
  - [Experiment 3 — Failure-Aware Retry](#experiment-3--failure-aware-retry)
  - [Experiment 4 — Sequential vs Parallel Execution](#experiment-4--sequential-vs-parallel-execution)
- [Source Code Changes](#source-code-changes)
- [Configuration Changes](#configuration-changes)
- [How to Run the Project](#how-to-run-the-project)
- [Technologies Used](#technologies-used)
- [Key Concepts Covered](#key-concepts-covered)
- [Learning Outcomes](#learning-outcomes)

---

## Project Overview

This project studies how Apache Airflow internally handles:

- DAG parsing and scheduling
- Workflow orchestration
- Task execution lifecycle
- Retry-based fault tolerance
- Failure recovery
- Execution tracing and observability

The project was developed as part of a Big Data Engineering systems analysis assignment focused on reverse engineering a real-world distributed workflow orchestration system.

---

## Project Objective

The goal is not simply to *use* Airflow, but to analyze its **internal system behavior** from a systems engineering perspective.

The project investigates:

- How Airflow orchestrates workflows
- How retries are implemented internally
- How task failures are handled
- What design tradeoffs exist
- How the system behaves under failure and load conditions

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

## 1. Experiment 1 — Airflow Task Lifecycle and Retry Behavior

### Purpose
Demonstrate Apache Airflow’s task lifecycle and understand how the retry mechanism works when a task fails during execution.

### What Was Observed
- Airflow task state transitions:

  ```text
  RUNNING → FAILED → UP_FOR_RETRY → RUNNING
  ```

- Automatic retry handling by Airflow
- How failed tasks are rescheduled
- The role of `retries` and `retry_delay`
- Changes in `try_number` during execution
- Scheduler behavior during retries

### Retry Behavior Observed

```text
Try 1 → FAILED
Try 2 → FAILED
Try 3 → SUCCESS (or final FAILED)
```

**DAG file:** `airflow_home/dags/exp1_default.py`

**Source file modified:** `airflow_src/models/taskinstance.py`

---

### Experiment 2 — Priority-Based Retry

**Purpose:** Implement a custom priority-based retry mechanism where a task's `priority_weight` determines how quickly it is retried.

**Priority levels and their behavior:**

| Priority Weight | Level  | Retry Delay Multiplier | Effect          |
|-----------------|--------|------------------------|-----------------|
| ≥ 10            | HIGH   | 0.5×                   | Faster retry    |
| 5 – 9           | MEDIUM | 1×                     | Normal retry    |
| < 5             | LOW    | 2×                     | Slower retry    |

**What was observed:**
- High-priority tasks recover much faster after failure
- Low-priority tasks wait longer between retries, freeing resources for critical tasks
- `priority_weight` is a native Airflow field, requiring no schema changes

**DAG file:** `airflow_home/dags/exp_priority_retry.py`

**Source file modified:** `airflow_src/models/taskinstance.py`

---

### Experiment 3 — Failure-Aware Retry

### Purpose
Implement intelligent retry logic where transient failures (such as network-related issues) are retried automatically, while permanent logic errors fail immediately without consuming unnecessary retry attempts.

### What Was Observed
- `AirflowException` triggers Airflow retry behavior
- `AirflowFailException` forces immediate final failure
- Retry logic can be dynamically controlled using exception types
- Unnecessary retries for permanent failures were avoided

### Logic Error Behavior (No Retry)

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

### Network Error Behavior (Retry Enabled)

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

**No source code changes** were made for this experiment. The behavior was achieved entirely through DAG-level logic.

**DAG file:** `airflow_home/dags/exp_failure_aware_retry.py`

---

### Experiment 4 — Sequential vs Parallel Execution

**Purpose:** Observe and compare task execution behavior under `SequentialExecutor` vs `LocalExecutor`.

**What was changed:**
- `executor` setting in `airflow.cfg` switched from `SequentialExecutor` to `LocalExecutor`
- Database backend switched from SQLite to PostgreSQL (required for parallel execution)

**What was observed:**
- `SequentialExecutor` runs one task at a time — simple but a bottleneck under load
- `LocalExecutor` spawns separate processes per task — true parallelism within a single machine
- SQLite cannot handle concurrent writes, making it incompatible with `LocalExecutor`

**Config file modified:** `airflow_home/airflow.cfg`

---

## Source Code Changes

### File: `airflow_src/models/taskinstance.py`

This is the core Airflow source file modified across Experiments 1 and 2. It contains the `TaskInstance` class responsible for managing task state, retry logic, and failure handling.

---

#### Experiment 1 — Modified Function: `next_retry_datetime()`

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

**Key change:** `delay_seconds * (2 ** (self.try_number - 1))` replaces a simple fixed multiplier. On attempt 1 delay is unchanged, on attempt 2 it doubles, on attempt 3 it quadruples, and so on.

---

#### Experiment 2 — Modified Functions: `next_retry_datetime()` and `fetch_handle_failure_context()`

**Class:** `TaskInstance`

**Why two functions were changed:** `next_retry_datetime()` computes the *when* of the next retry, while `fetch_handle_failure_context()` is the central failure handler that determines task state and retry eligibility. Both needed to reflect priority-based delay to ensure consistent behavior.

---

**Modified `next_retry_datetime()`:**

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

---

**Modified `fetch_handle_failure_context()`:**

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

**Why this mattered:** Without updating `fetch_handle_failure_context()`, the task's stored `retry_delay` would remain at its original value, causing a mismatch between the logged delay and the actual scheduling time.

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

This project demonstrates how Apache Airflow implements reliable workflow orchestration through retries, scheduling, and state-based execution management. By modifying Airflow's core `TaskInstance` class and experimenting with different executor and database configurations, we gained hands-on understanding of how distributed workflow systems balance reliability, performance, and fault tolerance.
