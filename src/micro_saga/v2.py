import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from typing import Any, Callable, List, Optional, ParamSpec, TypeVar, Union

P = ParamSpec("P")
R = ParamSpec("R")

T = TypeVar("T", bound="Operation")

# Note: inspect.iscoroutinefunction is called on every check; consider caching results for performance


def _is_coroutine_callable(fn: Callable) -> bool:
    import inspect

    return inspect.iscoroutinefunction(fn)


class Operation:
    action: Callable[[], Any]
    compensation: Optional[Callable[[], Any]] = None

    def __init__(self, op: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> None:
        # TODO: Consider validating that 'op' is a sync function; mixing async here may confuse behavior
        self.action = partial(op, *args, **kwargs)

    def add_compensation(
        self: T, op: Callable[R, Any], *args: R.args, **kwargs: R.kwargs
    ) -> T:
        self.compensation = partial(op, *args, **kwargs)
        return self

    class Config:
        arbitrary_types_allowed = True


class AsyncOperation(Operation):
    def __init__(self, op: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> None:
        # op expected to be async; not calling super().__init__ may skip important setup
        self.action = partial(op, *args, **kwargs)

    def add_compensation(
        self: T, op: Callable[R, Any], *args: R.args, **kwargs: R.kwargs
    ) -> T:
        # compensation may also be async; same concern about skipping superclass init
        self.compensation = partial(op, *args, **kwargs)
        return self


class SagaAssembler:
    """
    Saga assembler supporting sync and async operations.
    """

    def __init__(self, retry_attempts: int | None = None) -> None:
        self.__retry_attempts: int = retry_attempts or 1
        self.__operations: List[Operation] = []
        self.__saga_results: List[Any] = []

    class SagaException(Exception):
        def __init__(
            self,
            operation_error: Exception,
            operation_name: Optional[str] = None,
            compensation_success_result: Any | None = None,
            compensation_error: Any | None = None,
        ) -> None:
            self.operation_name = operation_name
            self.operation_error = operation_error
            self.compensation_success_result = compensation_success_result
            self.compensation_errors = compensation_error
            super().__init__(str(operation_error))

    @staticmethod
    def saga(retry_attempts: Optional[int] = None) -> "SagaAssembler":
        return SagaAssembler(retry_attempts=retry_attempts)

    def operation(self, operation: Operation) -> "SagaAssembler":
        self.__operations.append(operation)
        return self

    def __check_available_operations(self) -> None:
        if not self.__operations:
            raise self.SagaException(
                Exception("Set operation first to execute the saga.")
            )

    # --- Sync retry wrapper ---
    def __retry_operation(self, func: Callable[[], Any]) -> Any:
        last_exception: Optional[Exception] = None
        for _ in range(self.__retry_attempts):
            try:
                setattr(func, "saga_results", self.__saga_results)
                response = func()
                self.__saga_results.append(response)
                return response
            except Exception as err:
                last_exception = err
        raise last_exception or Exception("Unknown error in __retry_operation.")

    # --- Async retry wrapper ---
    async def __retry_operation_async(self, func: Callable[[], Any]) -> Any:
        last_exception: Optional[Exception] = None
        for _ in range(self.__retry_attempts):
            try:
                setattr(func, "saga_results", self.__saga_results)
                coro = func()
                response = await coro
                self.__saga_results.append(response)
                return response
            except Exception as err:
                last_exception = err
        raise last_exception or Exception("Unknown error in __retry_operation_async.")

    # --- Orchestrator (sync) ---
    def orchestrator_execute(self) -> List[Any]:
        self.__check_available_operations()
        responses: List[Any] = []
        for op_index, op in enumerate(self.__operations):
            try:
                # Accessing partial.func might fail if non-partial was passed; use getattr safely
                action_fn = getattr(op.action, "func", op.action)
                if _is_coroutine_callable(action_fn):
                    # asyncio.run spins up a new loop each call; consider reusing event loop
                    result = asyncio.run(self.__retry_operation_async(op.action))
                else:
                    result = self.__retry_operation(op.action)
                responses.append(result)
            except Exception as operation_error:
                comp_success, comp_errors = self.__execute_orchestrator_compensation(
                    op_index
                )
                raise self.SagaException(
                    operation_error,
                    (action_fn.__name__),
                    comp_success,
                    comp_errors,
                )
        return responses

    # --- Orchestrator (async) ---
    async def orchestrator_execute_async(self) -> List[Any]:
        self.__check_available_operations()
        responses: List[Any] = []
        for op_index, op in enumerate(self.__operations):
            try:
                action_fn = getattr(op.action, "func", op.action)
                if _is_coroutine_callable(action_fn):
                    result = await self.__retry_operation_async(op.action)
                else:
                    result = self.__retry_operation(op.action)
                responses.append(result)
            except Exception as operation_error:
                (
                    comp_success,
                    comp_errors,
                ) = await self.__execute_orchestrator_compensation_async(op_index)
                raise self.SagaException(
                    operation_error,
                    action_fn.__name__,
                    comp_success,
                    comp_errors,
                )
        return responses

    # --- Compensation helpers (sync) ---
    def __execute_orchestrator_compensation(
        self, last_operation_index: int
    ) -> tuple[Optional[List[Any]], Optional[List[Any]]]:
        success: List[Any] = []
        errors: List[str] = []
        for idx in range(last_operation_index - 1, -1, -1):
            op = self.__operations[idx]
            if op.compensation:
                try:
                    res = op.compensation()
                    success.append(res)
                except Exception as comp_err:
                    errors.append(str(comp_err))
        return (success or None, errors or None)

    # --- Compensation helpers (async) ---
    async def __execute_orchestrator_compensation_async(
        self, last_operation_index: int
    ) -> tuple[Optional[List[Any]], Optional[List[Any]]]:
        tasks = []
        for idx in range(last_operation_index - 1, -1, -1):
            op = self.__operations[idx]
            if op.compensation:
                comp_fn = getattr(op.compensation, "func", op.compensation)
                if _is_coroutine_callable(comp_fn):
                    tasks.append(comp_fn())
                else:
                    # wrap sync compensation in thread for parallelism
                    tasks.append(asyncio.to_thread(comp_fn))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success, errors = [], []
        for r in results:
            if isinstance(r, Exception):
                errors.append(str(r))
            else:
                success.append(r)
        return (success or None, errors or None)

    # --- Choreography (sync) ---
    class __SagaThreadExecutor(ThreadPoolExecutor):
        def submit(  # type: ignore
            self, *args, func_index: int, func_name: str, **kwargs
        ) -> dict:
            future: Future = super().submit(*args, **kwargs)
            return {
                "function_index": func_index,
                "function": future,
                "function_name": func_name,
            }

    class __SagaThreadException(Exception):
        def __init__(
            self, success_results: List[int], failed_results: List[Any]
        ) -> None:
            self.success_results = success_results
            self.failed_results = failed_results
            super().__init__("Saga Thread Exception occurred.")

    def choreography_execute(self) -> List[Any]:
        # asyncio.run here may conflict if called from running loop; consider exposing sync/async only
        return asyncio.run(
            self.choreography_execute_async(), loop_factory=asyncio.get_event_loop
        )

    async def choreography_execute_async(self) -> List[Any]:
        self.__check_available_operations()
        # launch all actions concurrently
        tasks = []
        for idx, op in enumerate(self.__operations):
            action_fn = getattr(op.action, "func", op.action)
            if _is_coroutine_callable(action_fn):
                tasks.append(
                    (
                        idx,
                        asyncio.create_task(
                            self.__retry_operation_async(op.action),
                            name=action_fn.__name__,
                        ),
                    )
                )
            else:
                # wrap sync in thread; spawning many threads may degrade performance
                tasks.append(
                    (idx, asyncio.to_thread(self.__retry_operation, op.action))
                )

        # gather results
        done, _ = await asyncio.wait(
            [t for _, t in tasks], return_when=asyncio.FIRST_EXCEPTION
        )
        errors = []  # collect first failure
        success_indices = []
        results_map = {}
        for idx, task in tasks:
            if task in done and task.exception():
                errors.append((idx, task.get_name(), task.exception()))
            elif not task.exception():
                results_map[idx] = task.result()
                success_indices.append(idx)
        if errors:
            (
                comp_success,
                comp_errors,
            ) = await self.__execute_choreography_compensation_async(success_indices)
            idx, name, err = errors[0]
            raise self.SagaException(err, name, comp_success, comp_errors)
        return [results_map[i] for i, _ in tasks]

    async def __execute_choreography_compensation_async(
        self, success_indices: List[int]
    ) -> tuple[Optional[List[Any]], Optional[List[Any]]]:
        tasks = []
        for idx in success_indices:
            op = self.__operations[idx]
            if op.compensation:
                comp_fn = getattr(op.compensation, "func", op.compensation)
                if _is_coroutine_callable(comp_fn):
                    tasks.append(comp_fn())
                else:
                    tasks.append(asyncio.to_thread(comp_fn))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success, errors = [], []
        for r in results:
            if isinstance(r, Exception):
                errors.append(str(r))
            else:
                success.append(r)
        return (success or None, errors or None)
