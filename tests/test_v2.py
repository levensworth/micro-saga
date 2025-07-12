import pytest

from micro_saga.v2 import AsyncOperation, SagaAssembler, Operation


def always_succeed(x):
    return x


async def async_always_succeed(x):
    return x


def always_fail():
    raise ValueError("fail")


async def async_always_fail():
    raise ValueError("fail")


def comp_ok():
    return "compensated"


async def async_comp_ok():
    return "compensated"


def comp_fail():
    raise RuntimeError("comp_fail")


async def async_comp_fail():
    raise RuntimeError("comp_fail")


@pytest.mark.asyncio
async def test_choreography_with_compensation_async():
    op1 = AsyncOperation(async_always_succeed, 5).add_compensation(async_comp_ok)
    op2 = Operation(always_fail)
    saga = SagaAssembler.saga().operation(op1).operation(op2)
    with pytest.raises(SagaAssembler.SagaException) as excinfo:
        saga.choreography_execute()
    exc = excinfo.value
    assert isinstance(exc.operation_error, ValueError)
    assert exc.compensation_success_result == ["compensated"]
    assert exc.compensation_errors is None
