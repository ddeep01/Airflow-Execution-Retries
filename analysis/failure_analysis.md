# Failure Analysis

This project analyzes how Apache Airflow behaves under task failures, retry conditions, and increased execution time.

The focus is on understanding how the Scheduler and TaskInstance system react when workflow execution does not proceed normally.

---

# 1. What Happens When a Component Fails?

In this project, failure is intentionally triggered inside `task_1()` using:

```python id="4f8l2n"
raise Exception("Simulated Failure")
```

This simulates a task-level execution failure.

---

## Failure Handling Flow

When the exception occurs:

1. The running task throws an error
2. Airflow catches the exception
3. Task state changes to `FAILED`
4. Retry eligibility is checked
5. State changes to `UP_FOR_RETRY`
6. Scheduler re-queues the task after `retry_delay`

Observed execution:

```text id="9v6q3m"
Try 1 → FAILED
Try 2 → FAILED
Try 3 → SUCCESS
```

### Internal File Reference

```text id="5x1w7r"
airflow/models/taskinstance.py
```

### Important Function

```python id="3k8n5v"
_handle_failure()
```

---

## Behavior After Retry Exhaustion

If all retries fail:

* Task is marked permanently as `FAILED`
* DAG execution cannot proceed normally
* Downstream dependent tasks are blocked

Observed downstream behavior:

```text id="7m4c1a"
UPSTREAM_FAILED
```

This ensures workflow correctness by preventing dependent tasks from executing with incomplete upstream data.

---

## System-Level Insight

This demonstrates Airflow’s fault tolerance strategy:

* Failures are isolated at task level
* Automatic retries reduce manual intervention
* Workflow consistency is preserved using dependency enforcement

However, repeated failures increase resource usage and execution latency.

---

# 2. What Happens When Execution Time Increases?

The project simulates long-running execution using:

```python id="2p9x6s"
time.sleep(10)
```

This artificially increases task runtime.

---

## Observed Behavior

As execution time increases:

* Worker processes remain occupied longer
* Retry completion becomes slower
* DAG completion time increases
* Scheduler waits longer before downstream scheduling

Execution pattern:

```text id="0r8q2j"
Longer task runtime
        ↓
Delayed retry completion
        ↓
Delayed downstream execution
        ↓
Higher overall DAG latency
```

---

## Scheduler Impact

Airflow’s Scheduler continuously monitors running tasks.

Long-running tasks can lead to:

* Reduced worker availability
* Increased task queue waiting time
* Lower workflow throughput
* Delayed scheduling decisions

This becomes more significant when multiple DAGs run simultaneously.

---

## System-Level Insight

This reflects a common distributed systems tradeoff:

```text id="1c7v9e"
Higher reliability through retries
        vs
Longer execution latency
```

Retries improve fault tolerance but increase overall execution cost when tasks are slow or repeatedly failing.

---

# 3. What Assumptions Does Airflow Rely On?

Airflow’s retry mechanism depends on several important assumptions.

---

## Assumption 1: Tasks Are Idempotent

Airflow assumes tasks can safely run multiple times.

This is critical because retries may execute the same task repeatedly.

Example:

```text id="8d3q7n"
Task fails → Retry triggered → Task executes again
```

If tasks are not idempotent, retries may cause:

* Duplicate database writes
* Duplicate file generation
* Data inconsistency

---

## Assumption 2: Failures Are Temporary

Retries are effective only when failures are transient.

Examples:

* Temporary network issue
* Short service outage
* Resource contention

Retries cannot fix permanent logic errors such as:

```python id="6t4m8k"
Syntax errors
Incorrect business logic
Invalid dependencies
```

Persistent failures eventually exhaust retry attempts.

---

## Assumption 3: DAG Structure Is Correct

Airflow assumes workflow dependencies are properly defined.

Example:

```python id="7j5w2p"
task_1 >> task_2
```

Incorrect dependency configuration can lead to:

* Deadlocks
* Skipped execution
* Invalid scheduling order

The Scheduler depends entirely on DAG correctness.

---

## Assumption 4: Metadata Database Remains Consistent

Airflow relies heavily on the metadata database (`airflow.db`) for:

* Task state tracking
* Retry counts
* Scheduler coordination
* DAG execution history

If metadata storage becomes inconsistent or unavailable:

* Retries may fail
* State tracking may break
* Scheduler behavior becomes unreliable

This creates a central dependency in the system architecture.

---

# 4. Failure Under Larger Scale Conditions

Although this project uses a small DAG, larger-scale workflows can introduce additional problems.

Potential issues include:

* Retry storms during widespread failures
* Scheduler bottlenecks
* Worker exhaustion
* Increased metadata database load
* Delayed retry scheduling

Example scenario:

```text id="0m5z8r"
Many tasks fail simultaneously
        ↓
All tasks retry together
        ↓
Worker queue overload
        ↓
Execution slowdown
```

This demonstrates scalability limitations in workflow orchestration systems.

---

# Conclusion

This project demonstrates how Apache Airflow handles workflow failures using retries and scheduler-based recovery.

Key observations include:

* Failed tasks are automatically retried
* Downstream execution is blocked until recovery succeeds
* Longer execution time increases workflow latency
* Retry mechanisms rely on strong assumptions such as idempotency and temporary failure conditions
* Large-scale failures can create scheduling and resource bottlenecks

The analysis highlights the tradeoff between reliability and execution efficiency in distributed workflow orchestration systems.
