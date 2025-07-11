
import pytest
from micro_saga import SagaAssembler, Operation, AsyncOperation

# Dummy functions for testing

def always_succeed(x):
    return x

def always_fail():
    raise ValueError("fail")

def comp_ok():
    return "compensated"

def comp_fail():
    raise RuntimeError("comp_fail")

# Test orchestrator happy path
def test_orchestrator_happy_path():
    saga = SagaAssembler.saga() \
        .operation(Operation(always_succeed, 1)) \
        .operation(Operation(always_succeed, 2))
    result = saga.orchestrator_execute()
    assert result == [1, 2]

# Test orchestrator with successful compensation
def test_orchestrator_with_compensation_success():
    op1 = Operation(always_succeed, 1).add_compensation(comp_ok)
    op2 = Operation(always_fail).add_compensation(comp_ok)
    saga = SagaAssembler.saga() \
        .operation(op1) \
        .operation(op2)
    with pytest.raises(SagaAssembler.SagaException) as excinfo:
        saga.orchestrator_execute()
    exc = excinfo.value
    assert isinstance(exc.operation_error, ValueError)
    assert exc.compensation_success_result == ['compensated']
    assert exc.compensation_errors is None

# Test orchestrator with failing compensation
def test_orchestrator_compensation_error():
    op1 = Operation(always_succeed, 1).add_compensation(comp_fail)
    op2 = Operation(always_fail)
    saga = SagaAssembler.saga() \
        .operation(op1) \
        .operation(op2)
    with pytest.raises(SagaAssembler.SagaException) as excinfo:
        saga.orchestrator_execute()
    exc = excinfo.value
    assert exc.compensation_success_result is None
    assert exc.compensation_errors == ['comp_fail']

# Test retry logic succeeds after retry
def test_retry_success_after_retry():
    state = {'calls': 0}
    def sometimes():
        state['calls'] += 1
        if state['calls'] == 1:
            raise Exception('temp')
        return 'ok'
    op = Operation(sometimes)
    saga = SagaAssembler.saga(retry_attempts=2).operation(op)
    result = saga.orchestrator_execute()
    assert result == ['ok']
    assert state['calls'] == 2

# Test retry logic fails after all attempts
def test_retry_failure():
    def always_bad():
        raise Exception('bad')
    op = Operation(always_bad)
    saga = SagaAssembler.saga(retry_attempts=3).operation(op)
    with pytest.raises(Exception) as excinfo:
        saga.orchestrator_execute()
    assert str(excinfo.value) == 'bad'

# Test choreography happy path
def test_choreography_happy_path():
    saga = SagaAssembler.saga() \
        .operation(Operation(always_succeed, 3)) \
        .operation(Operation(always_succeed, 4))
    result = saga.choreography_execute()
    assert sorted(result) == [3, 4]

# Test choreography with compensation
def test_choreography_with_compensation():
    op1 = Operation(always_succeed, 5).add_compensation(comp_ok)
    op2 = Operation(always_fail)
    saga = SagaAssembler.saga() \
        .operation(op1) \
        .operation(op2)
    with pytest.raises(SagaAssembler.SagaException) as excinfo:
        saga.choreography_execute()
    exc = excinfo.value
    assert isinstance(exc.operation_error, ValueError)
    assert exc.compensation_success_result == ['compensated']
    assert exc.compensation_errors is None

# Test missing operations error
def test_no_operations_error():
    saga = SagaAssembler.saga()
    with pytest.raises(SagaAssembler.SagaException):
        saga.orchestrator_execute()
    with pytest.raises(SagaAssembler.SagaException):
        saga.choreography_execute()

# Test AsyncOperation behaves like Operation
def test_async_operation_happy():
    saga = SagaAssembler.saga() \
        .operation(AsyncOperation(always_succeed, 10))
    result = saga.orchestrator_execute()
    assert result == [10]
