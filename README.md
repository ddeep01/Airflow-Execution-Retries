# Airflow Execution Retries

A systems-level analysis project on Apache Airflow focused on workflow orchestration, task execution retries, scheduler behavior, fault tolerance, and pipeline reliability.

This project studies how Airflow internally handles:

* DAG parsing and scheduling
* Workflow orchestration
* Task execution lifecycle
* Retry-based fault tolerance
* Failure recovery
* Execution tracing and observability

The project was developed as part of a Big Data Engineering systems analysis assignment focused on reverse engineering a real-world distributed workflow orchestration system.

---

# Project Objective

The goal of this project is not simply to use Airflow, but to analyze its internal system behavior from a systems engineering perspective.

The project investigates:

* How Airflow orchestrates workflows
* How retries are implemented internally
* How task failures are handled
* What design tradeoffs exist
* How the system behaves under failure and load conditions

---

# Key Features

* DAG-based workflow execution
* Workflow orchestration analysis
* Automatic retry mechanism
* Failure simulation using exceptions
* Task execution tracing
* Scheduler and TaskInstance analysis
* Retry state transition observation
* Load simulation using delayed execution
* Internal execution flow analysis

---

# Project Structure

```text
Airflow-Execution-Retries/
│
├── airflow_home/                 # Airflow runtime directory
├── airflow_src/                  # Airflow source code references
│
├── analysis/
│   ├── concept_mapping.md
│   ├── design_decision.md
│   ├── execution.md
│   └── failure_analysis.md
│
├── experiments/
│   ├── exp1_default.py
│   ├── exp_failure_aware_retry.py
│   └── exp_priority_retry.py
│
├── results/                      # Experiment outputs/logs
│
├── requirements.txt
└── .gitignore
```

---

# System Overview

This project traces the following Airflow execution path:

```text
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

The workflow demonstrates how Airflow maintains reliability using retry-based fault tolerance.

---

# Data Orchestration Perspective

Apache Airflow is fundamentally a workflow orchestration system used for coordinating multi-stage data pipelines.

In this project, Airflow orchestrates task execution using:

* DAG-based dependency management
* Centralized scheduling
* Stateful task monitoring
* Retry-aware execution control

The workflow simulates a typical orchestration pipeline:

```text
Task Submission
    ↓
Dependency Resolution
    ↓
Task Scheduling
    ↓
Execution
    ↓
Failure Detection
    ↓
Retry Coordination
    ↓
Successful Pipeline Completion
```

Although the project uses simplified tasks, the execution model is similar to real-world data engineering pipelines such as:

* ETL workflows
* Batch ingestion pipelines
* Data transformation jobs
* Scheduled analytics pipelines

This project specifically demonstrates how orchestration systems maintain reliability using retry-based fault tolerance and dependency-aware scheduling.

---

# Retry Mechanism Demonstrated

The project configures retries using:

```python
default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}
```

Observed execution behavior:

```text
Try 1 → FAILED
Try 2 → FAILED
Try 3 → SUCCESS
```

State transitions observed:

```text
FAILED → UP_FOR_RETRY → RUNNING → SUCCESS
```

---

# Concepts Covered

This project maps Airflow internals to distributed systems concepts including:

* DAG-based execution models
* Workflow orchestration
* Fault tolerance and retries
* Scheduler-based coordination
* Metadata storage
* Batch pipeline orchestration
* Execution state management

---

# Design Decisions Analyzed

The project analyzes several important system design decisions:

* Retry-based fault tolerance
* Failure simulation using exceptions
* Monkey patching for observability
* Long-running task simulation
* Sequential dependency execution

Each decision is studied along with its implementation and tradeoffs.

---

# Failure Analysis

The project studies system behavior under:

* Task failures
* Retry exhaustion
* Increased execution time
* Delayed scheduling
* Downstream dependency failure

Key assumptions analyzed include:

* Task idempotency
* Temporary failure conditions
* Correct DAG dependencies
* Metadata database consistency

---

# Experiments Performed

The experiments folder contains different retry behavior experiments including:

| Experiment                   | Purpose                       |
| ---------------------------- | ----------------------------- |
| `exp1_default.py`            | Default retry behavior        |
| `exp_failure_aware_retry.py` | Failure-aware retry logic     |
| `exp_priority_retry.py`      | Priority-based retry handling |

These experiments help observe how Airflow reacts under different retry and scheduling conditions.

---

# Technologies Used

* Apache Airflow
* Python
* SQLite (Airflow metadata DB)
* LocalExecutor
* VS Code

---

# Learning Outcomes

Through this project, the following concepts were understood:

* Internal Airflow execution flow
* Workflow orchestration
* Scheduler-task coordination
* TaskInstance lifecycle
* Retry handling mechanisms
* Workflow reliability tradeoffs
* Distributed workflow orchestration concepts

---

# Detailed Analysis Files

Further detailed analysis can be found in the following markdown files:

* `analysis/concept_mapping.md`
* `analysis/design_decision.md`
* `analysis/execution.md`
* `analysis/failure_analysis.md`

---

# Conclusion

This project demonstrates how Apache Airflow implements reliable workflow orchestration using retries, scheduling, and state-based execution management.

The analysis highlights both the strengths and tradeoffs of retry-driven fault tolerance in distributed workflow systems.
