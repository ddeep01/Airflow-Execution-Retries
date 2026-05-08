# Design Decisions

This project focuses on understanding Apache Airflow’s retry execution mechanism through controlled experimentation and instrumentation.

The following design decisions were implemented to study workflow reliability, execution behavior, and failure handling.

---

# 1. Using Airflow Retry Mechanism

### Where Implemented

Inside the DAG configuration file:

```python id="e7nb9n"
default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}
```

### Problem It Solves

Distributed systems frequently experience temporary failures such as:

* Network instability
* Resource contention
* Delayed service response
* Temporary worker failure

Airflow’s retry mechanism automatically re-executes failed tasks instead of permanently failing the workflow immediately.

In this project, retries help demonstrate:

* Automatic recovery behavior
* Stateful task execution
* Scheduler-driven fault tolerance

Observed behavior:

```text id="wsm4f9"
Attempt 1 → FAILED
Attempt 2 → FAILED
Attempt 3 → SUCCESS
```

### Tradeoff

Retries improve reliability but introduce several tradeoffs:

* Increased total workflow execution time
* Additional scheduler overhead
* Resource consumption during repeated execution
* Possible masking of deeper system issues if failures are ignored

Excessive retries can also create retry storms under large-scale failures.

---

# 2. Simulating Failures Using Exceptions

### Where Implemented

Inside the task function:

```python id="tf8lga"
raise Exception("Simulated Failure")
```

### Problem It Solves

A controlled failure mechanism was introduced to intentionally trigger Airflow retries.

This helps in:

* Observing retry lifecycle
* Understanding scheduler behavior
* Tracking state transitions
* Studying task recovery mechanisms

The project uses deterministic failures so execution behavior can be reproduced consistently.

### Tradeoff

Artificial failures simplify experimentation but differ from real-world failures.

Limitations include:

* Does not simulate distributed network failures
* Does not model partial worker crashes
* No resource exhaustion simulation
* Failure conditions are predictable

Therefore, results represent conceptual retry behavior rather than production-scale reliability testing.

---

# 3. Monkey Patching TaskInstance for Execution Tracing

### Where Implemented

File:

```text id="szb5r9"
instrumentation/debug_hooks.py
```

Implementation:

```python id="zjvh7t"
from airflow.models.taskinstance import TaskInstance

original_run = TaskInstance.run

def debug_run(self, *args, **kwargs):
    print(f"[DEBUG] Task START: {self.task_id}")
    result = original_run(self, *args, **kwargs)
    print(f"[DEBUG] Task END: {self.task_id}")
    return result

TaskInstance.run = debug_run
```

### Problem It Solves

Airflow abstracts much of its internal execution flow.

Monkey patching was used to:

* Trace task execution lifecycle
* Observe internal function calls
* Improve observability
* Understand execution boundaries

This provided visibility into:

* Task start timing
* Retry execution order
* Task completion events

### Tradeoff

Monkey patching modifies internal framework behavior directly.

Risks include:

* Unexpected side effects
* Compatibility issues after Airflow updates
* Difficult debugging if internal APIs change
* Non-production-safe modifications

This technique is useful for educational analysis but generally avoided in production systems.

---

# 4. Simulating Long-Running Tasks Using Sleep

### Where Implemented

Inside task execution logic:

```python id="9t7c0q"
import time
time.sleep(10)
```

### Problem It Solves

Artificial execution delay was introduced to simulate long-running workloads.

This helps observe:

* Scheduler coordination behavior
* Worker occupation duration
* Retry timing
* DAG execution latency

The delay also makes retry transitions easier to visualize in logs and UI.

### Tradeoff

Using `sleep()` is useful for experimentation but inefficient for real systems.

Limitations include:

* Idle resource consumption
* No actual workload processing
* Increased pipeline runtime
* Unrealistic compute characteristics

The delay only simulates execution time, not true computational load.

---

# 5. Sequential Task Dependency Design

### Where Implemented

Inside DAG task dependency definition:

```python id="22ax2j"
task_1 >> task_2
```

### Problem It Solves

Sequential dependency ensures:

* Downstream tasks execute only after upstream success
* Retry recovery completes before pipeline continuation
* Failure propagation can be observed clearly

This simplifies retry analysis and execution tracing.

### Tradeoff

Sequential execution reduces workflow parallelism.

Effects include:

* Lower throughput
* Increased total DAG runtime
* Underutilization of available workers

However, it improves observability and makes retry behavior easier to study.

---

# Conclusion

The design decisions in this project were chosen primarily to study Airflow’s retry execution mechanism and internal scheduling behavior.

The project demonstrates:

* Reliability through retries
* Controlled failure experimentation
* Internal execution tracing
* Scheduler behavior under delayed execution
* Dependency-driven workflow control

These decisions improve understanding of distributed workflow systems, while also introducing tradeoffs such as increased runtime, artificial workload behavior, and non-production-safe instrumentation techniques.
