# Concept Mapping

This project using Apache Airflow is mapped only to the distributed system concepts that are directly applicable to retry execution behavior.

---

# 1. Execution Model (DAG-Based Scheduling)

Apache Airflow uses a Directed Acyclic Graph (DAG) to define workflow execution.

In a DAG:

* Tasks are represented as nodes
* Dependencies are represented as edges
* Tasks execute in topological order

In this project:

```python
task_1 >> task_2
```

This ensures:

* `task_1` executes before `task_2`
* `task_2` runs only if upstream conditions are satisfied

The Scheduler continuously checks task states and schedules runnable tasks.

### Code Reference

Retry behavior is configured inside the DAG definition:

```python
retries=2
retry_delay=timedelta(seconds=30)
```

### Mapping

This directly maps to DAG execution models used in distributed systems such as:

* Apache Spark
* Apache Tez
* Workflow orchestration systems

The retry mechanism becomes part of the DAG execution lifecycle.

---

# 2. Reliability and Fault Tolerance

The core focus of this project is Airflow’s retry-based fault tolerance mechanism.

When a task fails:

1. Task state changes to `FAILED`
2. Scheduler marks it as `UP_FOR_RETRY`
3. Task is rescheduled after `retry_delay`
4. Execution continues until retries are exhausted or task succeeds

Observed execution:

```text
Try 1 → FAILED
Try 2 → FAILED
Try 3 → SUCCESS
```

### State Transition

```text
FAILED → UP_FOR_RETRY → RUNNING → SUCCESS
```

### Mapping

This demonstrates fault tolerance concepts used in distributed systems:

* Automatic recovery from transient failures
* Resilient workflow execution
* Failure isolation
* Controlled re-execution

Airflow prevents downstream tasks from executing until retry recovery succeeds.

This is similar to retry handling in:

* Spark task execution
* Message queue consumers
* Distributed job schedulers

---

# 3. Metadata Storage (B-tree Based Database Storage)

Airflow stores workflow metadata inside its metadata database (`airflow.db`).

The metadata database stores:

* DAG runs
* Task states
* Retry counts
* Execution timestamps
* Scheduler metadata

Important retry-related fields include:

```text
try_number
state
start_date
end_date
```

### Mapping

Airflow commonly uses SQLite or PostgreSQL.

These databases internally use B-tree indexing for:

* Fast task lookup
* Efficient scheduling queries
* State tracking
* Retry management

This maps directly to storage concepts taught in distributed systems:

* Indexed metadata storage
* Persistent execution tracking
* Efficient state retrieval

---

# 4. Batch Pipeline Orchestration

Airflow is fundamentally a batch workflow orchestration system.

In this project:

* Tasks simulate stages of a processing pipeline
* Scheduler coordinates execution
* Retry logic handles temporary execution failures

Execution flow:

```text
Task Submission → Scheduler → Worker Execution → Retry Handling
```

### Mapping

This maps to batch ingestion and orchestration pipelines where:

* Jobs execute in stages
* Failures are automatically retried
* Dependencies are enforced centrally

Airflow acts as an orchestration layer rather than a data processing engine itself.

---

# 5. Scheduler-Based Distributed Coordination

Airflow’s Scheduler acts as a coordination component.

Responsibilities include:

* Monitoring task states
* Detecting failures
* Triggering retries
* Managing execution timing

In this project:

* Scheduler detects failed task instances
* Retry attempts are automatically scheduled
* Delay between retries is enforced

### Mapping

This reflects distributed coordination concepts:

* Centralized scheduling
* State-based execution management
* Failure-aware orchestration

This behavior is similar to cluster coordinators used in distributed systems.

---

# Conclusion

This project demonstrates several important distributed system concepts through Apache Airflow’s retry mechanism:

* DAG-based workflow execution
* Fault tolerance through retries
* Metadata storage using indexed databases
* Batch pipeline orchestration
* Scheduler-based coordination

The project shows how Airflow maintains reliable workflow execution even when task failures occur temporarily.
