from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Protocol

from aiodynamo.errors import AIODynamoError


class HealthMonitor(Protocol):
    """
    Protocol for health monitors that can be configured on clients.

    These can be used to detect unhealthy instances on AWS that can no longer communicate with DynamoDB.
    """

    def is_healthy(self) -> bool:
        """
        Returns whether the client should be considered healthy or not.
        """

    def on_exception(self, exc: Exception) -> None:
        """
        A request sent to DynamoDB resulted in an exception.
        """

    def on_success(self) -> None:
        """
        A request sent to DynamoDB succeeded.
        """


class OnSuccess(Enum):
    decrement = auto()
    reset = auto()
    noop = auto()


@dataclass(kw_only=True)
class CountingHealthMonitor:
    """
    A simple health monitor that counts any exception (other than those defined in `ignore_exceptions`).
    Your system should monitor the `healthy` property to see if this client is considered healthy.
    Adjust the number of failures that need to happen before it gets marked as unhealthy using the
    `max_failures` property.
    `on_success_action` controls what should happen when a successful request finished.
    """

    max_failures: int = 5
    ignore_exceptions: tuple[type[Exception]] = (AIODynamoError,)
    on_success_action: OnSuccess = OnSuccess.decrement
    _count: int = field(init=False, default=0)

    def is_healthy(self) -> bool:
        return self._count < self.max_failures

    def on_exception(self, exc: Exception) -> None:
        if isinstance(exc, self.ignore_exceptions):
            return
        self._count += 1

    def on_success(self) -> None:
        match self.on_success_action:
            case OnSuccess.decrement:
                self._count = max(self._count - 1, 0)
            case OnSuccess.reset:
                self._count = 0
            case OnSuccess.noop:
                pass


class CallStrategy(Enum):
    edge = auto()
    always = auto()
    once = auto()


@dataclass
class CallbackHealthMonitor:
    """
    A health monitor wrapping another health monitor and calling the provided callback according to the
    `callback_strategy`. The callback takes no arguments and its return value is ignored.
    """

    inner: HealthMonitor
    callback: Callable[[], None]
    call_strategy: CallStrategy = CallStrategy.edge
    _was_healthy: bool = field(init=False, default=True)
    _did_call: bool = field(init=False, default=False)

    def is_healthy(self) -> bool:
        return self.inner.is_healthy()

    def _call(self) -> None:
        self.callback()
        self._did_call = True

    def on_exception(self, exc: Exception) -> None:
        self.inner.on_exception(exc)
        healthy = self.is_healthy()
        if not healthy:
            match self.call_strategy:
                case CallStrategy.edge:
                    if self._was_healthy:
                        self._call()
                case CallStrategy.always:
                    self._call()
                case CallStrategy.once:
                    if not self._did_call:
                        self._call()
        self._was_healthy = healthy

    def on_success(self) -> None:
        self.inner.on_success()
        self._was_healthy = self.is_healthy()


class NoOpHealthMonitor:
    """
    Noop health monitor. Doesn't do anything and always reports as healthy.
    """

    def is_healthy(self) -> bool:
        return True

    def on_exception(self, exc: Exception) -> None:
        pass

    def on_success(self) -> None:
        pass
