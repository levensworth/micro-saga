
# Micro Saga

A minimal implementation of a retry mechanism based on the Sagas Pattern.

## Features

- Minimal, type-annotated implementation of the Saga Pattern for Python.
- Supports both **orchestrated** (sequential) and **choreographed** (concurrent/threaded) saga execution.
- Built-in retry mechanism for each operation.
- Compensation actions for rollback on failure.
- Custom exceptions for error handling and compensation reporting.
- Support for `async` operations.
- No dependencies!

## Installation

TBD

## Usage

### Defining Operations

```python
from micro_saga import Operation, SagaAssembler

def create_resource(x):
    print(f"Creating resource {x}")
    return x

def delete_resource(x):
    print(f"Deleting resource {x}")

op = Operation(create_resource, 42).add_compensation(delete_resource, 42)
```

### Orchestrated Saga (Sequential)

```python
saga = SagaAssembler.saga(retry_attempts=3)
saga.operation(op)
try:
    results = saga.orchestrator_execute()
    print("Saga succeeded:", results)
except SagaAssembler.SagaException as e:
    print("Saga failed:", e)
```

### Choreographed Saga (Concurrent)

```python
saga = SagaAssembler.saga(retry_attempts=2)
saga.operation(op)
try:
    results = saga.choreography_execute()
    print("Saga succeeded:", results)
except SagaAssembler.SagaException as e:
    print("Saga failed:", e)
```

### Adding Multiple Operations

```python
op1 = Operation(create_resource, 1).add_compensation(delete_resource, 1)
op2 = Operation(create_resource, 2).add_compensation(delete_resource, 2)

saga = SagaAssembler.saga()
saga.operation(op1).operation(op2)
```


### Async operations:

## API Reference

- `Operation(func, *args, **kwargs)`: Wraps an action with optional compensation.
- `add_compensation(func, *args, **kwargs)`: Adds a compensation to an operation.
- `SagaAssembler.saga(retry_attempts=None)`: Creates a new saga instance.
- `operation(op)`: Adds an operation to the saga.
- `orchestrator_execute()`: Runs operations sequentially, compensates on failure.
- `choreography_execute()`: Runs operations concurrently, compensates on failure.

## License

MIT License.