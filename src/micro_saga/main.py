from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from typing import Any, Callable, List, Optional, ParamSpec, TypeVar

P = ParamSpec("P")
R = ParamSpec("R")

T = TypeVar("T", bound="Operation")


class Operation:
    action: Callable[[], Any]
    compensation: Optional[Callable[[], Any]] = None

    def __init__(self, op: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> None:
        self.action = partial(op, *args, **kwargs)

    def add_compensation(
        self: T, op: Callable[R, Any], *args: R.args, **kwargs: R.kwargs
    ) -> T:
        self.compensation = partial(op, *args, **kwargs)
        return self

    class Config:
        arbitrary_types_allowed = True


class AsyncOperation(Operation):
    action: Callable[[], Any]
    compensation: Optional[Callable[[], Any]] = None

    def __init__(self, op: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> None:
        self.action = partial(op, *args, **kwargs)


class SagaAssembler:
    """
    Saga assembler to create saga.
    """

    def __init__(self, retry_attempts: int | None = None) -> None:
        self.__retry_attempts: int = retry_attempts or 1
        self.__operations: List[Operation] = []
        self.__saga_results: List[Any] = []

    class SagaException(Exception):
        """
        Raised when an operation action failed.
        """

        def __init__(
            self,
            operation_error: Exception,
            operation_name: str | None = None,
            compensation_success_result: Any | None = None,
            compensation_error: Any | None = None,
        ) -> None:
            self.operation_name = operation_name
            self.operation_error = operation_error
            self.compensation_success_result = compensation_success_result
            self.compensation_errors = compensation_error
            super().__init__(str(operation_error))

    class __SagaThreadExecutor(ThreadPoolExecutor):
        """
        Saga thread pool Executor.
        """

        def submit(  # type: ignore
            self,
            *args,
            func_index: int,
            func_name: str,
            **kwargs,
        ) -> dict:  # type: ignore
            future: Future = super().submit(*args, **kwargs)  # type: ignore
            return {
                "function_index": func_index,
                "function": future,
                "function_name": func_name,
            }  # type: ignore

    class __SagaThreadException(Exception):
        """
        Raised when a thread operation action failed.
        """

        def __init__(
            self, success_results: List[int], failed_results: List[Any]
        ) -> None:
            self.success_results = success_results
            self.failed_results = failed_results
            super().__init__("Saga Thread Exception occurred.")

    @staticmethod
    def saga(retry_attempts: Optional[int] = None) -> "SagaAssembler":
        """
        Create SagaAssembler instance.
        """
        return SagaAssembler(retry_attempts=retry_attempts)

    def operation(self, operation: Operation) -> "SagaAssembler":
        """
        Add an operation action and a corresponding compensation if provided.
        """
        self.__operations.append(operation)
        return self

    def __check_available_operations(self) -> None:
        """
        Check operation availability.
        """
        if not self.__operations:
            raise self.SagaException(
                Exception("Set operation first to execute the saga.")
            )

    def __retry_operation(self, func: Callable[[], Any]) -> Any:
        """
        Perform and retry an operation.
        """
        last_exception: Optional[Exception] = None
        for _ in range(self.__retry_attempts):
            try:
                # preserve saga_results in the function if needed
                setattr(func, "saga_results", self.__saga_results)
                response = func()
                self.__saga_results.append(response)
            except Exception as err:
                last_exception = err
            else:
                return response
        # If failed after all attempts, raise the last caught exception.
        if last_exception:
            raise last_exception
        # Fallback; this should never be reached.
        raise Exception("Unknown error in __retry_operation.")

    def orchestrator_execute(self) -> List[Any]:
        """
        Executes a series of Operation Actions sequentially.
        If one of the operation actions raises an exception, compensations for
        all previous successful operations are executed in reverse order.
        """
        self.__check_available_operations()
        responses: List[Any] = []
        for op_index, op in enumerate(self.__operations):
            try:
                responses.append(self.__retry_operation(op.action))
            except Exception as operation_error:
                comp_success, comp_errors = self.__execute_orchestrator_compensation(
                    op_index
                )
                raise self.SagaException(
                    operation_error,
                    op.action.func.__name__
                    if hasattr(op.action, "func")
                    else op.action.__name__,  # type: ignore
                    comp_success,
                    comp_errors,
                )
        return responses

    def __execute_orchestrator_compensation(
        self, last_operation_index: int
    ) -> tuple[Optional[List[Any]], Optional[List[Any]]]:
        """
        Execute compensations for successful operations up to last_operation_index (in reverse order).
        """
        compensation_success_result: List[Any] = []
        compensation_exceptions: List[str] = []
        for idx in range(last_operation_index - 1, -1, -1):
            op = self.__operations[idx]
            if op.compensation is not None:
                try:
                    compensation_success_result.append(op.compensation())
                except Exception as comp_error:
                    compensation_exceptions.append(str(comp_error))
        return compensation_success_result or None, compensation_exceptions or None

    def __prepare_thread_result(self, threads: List[dict[str, Any]]) -> List[Any]:
        """
        Gather results from futures executed in threads.
        """
        success: List[Any] = []
        success_indices: List[int] = []
        errors: List[Any] = []
        for thread in threads:  # type: ignore
            try:
                result = thread["function"].result()  # type: ignore
                success.append(result)
                success_indices.append(thread["function_index"])  # type: ignore
            except Exception as error:
                errors.append(
                    {
                        "function_index": thread["function_index"],
                        "function_name": thread["function_name"],
                        "error": error,
                    }
                )
        if errors:
            raise self.__SagaThreadException(
                success_results=success_indices, failed_results=errors
            )
        return success

    def choreography_execute(self) -> List[Any]:
        """
        Executes a series of Operation Actions concurrently via threads.
        If one of the operations fails, compensations for all successfully executed operations are run.
        """
        try:
            self.__check_available_operations()
            with self.__SagaThreadExecutor(
                max_workers=len(self.__operations)
            ) as pool_executor:
                thread_jobs: list[dict[str, Any]] = [
                    pool_executor.submit(  # type: ignore
                        partial(self.__retry_operation, op.action),
                        func_index=idx,
                        func_name=(
                            op.action.func.__name__  # type: ignore
                            if hasattr(op.action, "func")
                            else op.action.__name__
                        ),
                    )
                    for idx, op in enumerate(self.__operations)
                ]
                return self.__prepare_thread_result(thread_jobs)
        except self.__SagaThreadException as thread_exc:
            # In case of failure, execute compensation on the successfully run operations
            comp_success, comp_errors = self.__execute_choreography_compensation(
                thread_exc.success_results
            )
            first_failed = thread_exc.failed_results[0]
            raise self.SagaException(
                first_failed["error"],
                first_failed["function_name"],
                comp_success,
                comp_errors,
            )

    def __execute_choreography_compensation(
        self, success_indices: List[int]
    ) -> tuple[Optional[List[Any]], Optional[List[Any]]]:
        """
        Execute compensations for operations that succeeded, concurrently.
        """
        compensation_success_result: List[Any] = []
        compensation_exceptions: List[str] = []
        with self.__SagaThreadExecutor(
            max_workers=len(self.__operations)
        ) as pool_executor:
            comp_threads: list[dict[str, Any]] = [
                pool_executor.submit(  # type: ignore
                    self.__operations[idx].compensation,
                    func_index=idx,
                    func_name=(
                        self.__operations[idx].compensation.func.__name__  # type: ignore
                        if self.__operations[idx].compensation
                        and hasattr(self.__operations[idx].compensation, "func")
                        else (
                            self.__operations[idx].compensation.__name__  # type: ignore
                            if self.__operations[idx].compensation
                            else "NoCompensation"
                        )
                    ),  # type: ignore
                )
                for idx in success_indices
                if self.__operations[idx].compensation is not None
            ]
            for thread in comp_threads:
                try:
                    compensation_success_result.append(thread["function"].result())
                except Exception as error:
                    compensation_exceptions.append(str(error))
        return compensation_success_result or None, compensation_exceptions or None
