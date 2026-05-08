# Execution Understanding (Airflow Task Execution Flow)

---

# 1. Overview

This project traces the complete execution flow of an Apache Airflow DAG named `my_first_dag`.

The execution path analyzed is:

```text id="jlwm8o"
DAG Parsing → Scheduling → Task Execution → Failure Handling → Retry → Final Success
```

The purpose of this trace is to understand how Airflow internally manages workflow execution, task state transitions, and retry-based fault tolerance.

---

# 2. Complete Execution Path

---

# Step 1: DAG Parsing (DagBag)

When Airflow starts, it scans the DAG directory and loads all DAG definitions.

### File Reference

```text id="o1jbxv"
airflow/models/dagbag.py
```

### Important Function

```python id="6q7mr7"
DagBag.parse()
```

### Execution Flow

* Airflow reads `dags/my_first_dag.py`
* DAG object is created
* Tasks and dependencies are registered
* DAG is stored in memory for scheduling

### Purpose

This step converts Python workflow definitions into executable Airflow objects.

---

# Step 2: Scheduler Creates DAG Run

The Scheduler continuously monitors all parsed DAGs.

### File Reference

```text id="v8k6t9"
airflow/jobs/scheduler_job_runner.py
```

### Important Function

```python id="kp2d4e"
SchedulerJobRunner._run_scheduler_loop()
```

### Execution Flow

The scheduler:

* Detects runnable DAGs
* Creates a `DagRun`
* Creates `TaskInstance` objects
* Determines which tasks are eligible to run

### Purpose

This is the orchestration layer responsible for workflow coordination.

---

# Step 3: TaskInstance Initialization

Each task execution is represented internally using a `TaskInstance`.

### File Reference

```text id="g9v7ws"
airflow/models/taskinstance.py
```

### Important Attributes

```text id="r7x2n5"
task_id
state
try_number
max_tries
```

### Purpose

`TaskInstance` maintains execution metadata such as:

* Current task state
* Retry count
* Execution timestamps
* Failure information

This object is central to Airflow’s retry mechanism.

---

# Step 4: Task Sent to Executor

After scheduling, runnable tasks are sent to the Executor.

### Executor Used

```text id="m4e6pz"
LocalExecutor
```

### Execution Flow

* Scheduler queues the task
* Executor picks up the task
* Worker process starts execution

### Purpose

The executor separates scheduling from actual task execution.

---

# Step 5: Task Execution Begins

The worker initializes runtime execution.

### File Reference

```text id="6v3slt"
airflow/sdk/execution_time/task_runner.py
```

### Important Function

```python id="0vbrmq"
run()
```

### Internal Flow

```text id="npxo4j"
run() → _execute_task()
```

### Purpose

This layer:

* Prepares execution environment
* Loads task context
* Starts operator execution

---

# Step 6: PythonOperator Executes User Task

Airflow now invokes the operator responsible for running Python functions.

### File Reference

```text id="36kq2m"
airflow/providers/standard/operators/python.py
```

### Important Functions

```python id="1wq2dl"
execute()
execute_callable()
```

### Execution Flow

The operator calls the user-defined function:

```python id="5v9tfa"
task_1()
```

### Purpose

Operators act as execution wrappers around user-defined business logic.

---

# Step 7: User Function Execution

The actual workflow logic runs inside the DAG task.

### File Reference

```text id="wq0e7x"
dags/my_first_dag.py
```

### Function

```python id="frz5om"
def task_1(**context):
```

### Observed Behavior

The function:

* Prints execution logs
* Simulates workload
* Intentionally raises an exception

```python id="jlwm3n"
raise Exception("Simulated Failure")
```

### Purpose

This controlled failure is used to trigger Airflow’s retry mechanism.

---

# 3. Retry Execution Flow

---

# Step 8: Failure Handling

When the task raises an exception, Airflow catches the failure.

### File Reference

```text id="7xw9pb"
airflow/models/taskinstance.py
```

### Important Function

```python id="m5tk2o"
_handle_failure()
```

### Execution Flow

Airflow:

* Captures exception information
* Updates task state
* Records retry metadata

### State Transition

```text id="f9n2rk"
FAILED → UP_FOR_RETRY
```

### Purpose

This enables automatic recovery instead of immediate workflow termination.

---

# Step 9: Retry Scheduling

After failure, the Scheduler evaluates retry eligibility.

### Retry Condition

```text id="1b2o8m"
try_number <= max_tries
```

### Execution Flow

If retries remain:

* Scheduler delays execution using `retry_delay`
* Task is queued again
* New execution attempt starts

### Purpose

This provides resilience against temporary failures.

---

# Step 10: Task Re-execution

The task execution cycle repeats.

### Observed Runtime Behavior

```text id="ie7r6a"
Try 1 → FAILED
Try 2 → FAILED
Try 3 → SUCCESS
```

### Purpose

Airflow continues retrying until:

* Task succeeds
* Retry limit is exhausted

---

# 4. Final Success Path

On the final attempt, the task completes successfully.

### File Reference

```text id="8q7jtl"
airflow/models/taskinstance.py
```

### Important Function

```python id="4c2hke"
TaskInstance.set_state()
```

### Final State

```text id="3l5m7p"
SUCCESS
```

### Purpose

The Scheduler now allows downstream dependencies to execute.

---

# Step 11: Downstream Task Execution

Task dependencies are enforced using DAG edges.

### Dependency Definition

```python id="a4q8dv"
task_1 >> task_2
```

### Execution Flow

* `task_2` waits until `task_1` succeeds
* After successful retry recovery, downstream execution begins

### Purpose

This guarantees dependency correctness and prevents invalid pipeline execution.

---

# 5. Summary of Complete Execution Flow

```text id="1y4sop"
DagBag.parse()
    ↓
Scheduler creates DagRun
    ↓
TaskInstance created
    ↓
Task sent to Executor
    ↓
task_runner.run()
    ↓
PythonOperator.execute()
    ↓
task_1() runs
    ↓
Exception raised
    ↓
_handle_failure()
    ↓
Retry scheduled
    ↓
Re-execution
    ↓
Final SUCCESS
    ↓
task_2 executes
```

---

# 6. Conclusion

This execution trace demonstrates how Apache Airflow internally manages workflow execution and reliability.

The project shows how Airflow:

* Parses DAG definitions
* Schedules workflow execution
* Creates TaskInstances
* Executes tasks using operators
* Handles failures automatically
* Retries failed tasks
* Maintains execution state
* Enforces dependency ordering

The retry mechanism demonstrates Airflow’s fault tolerance model and highlights how workflow orchestration systems maintain reliable execution under transient failures.
