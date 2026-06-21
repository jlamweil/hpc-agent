"""
Runtime exceptions for async agent system.
"""

class RuntimeError(Exception):
    """Base exception for runtime errors."""
    pass


class TaskQueueError(RuntimeError):
    """Task queue operation failed."""
    pass


class QueueFullError(TaskQueueError):
    """Queue capacity exceeded."""
    pass


class StateStoreError(RuntimeError):
    """State persistence operation failed."""
    pass


class StateNotFoundError(StateStoreError):
    """Agent state does not exist."""
    pass


class StateCorruptedError(StateStoreError):
    """Agent state checksum failed."""
    pass


class AgentTimeoutError(RuntimeError):
    """Agent exceeded max wait time for LLM result."""
    pass


class AgentRetryExhaustedError(RuntimeError):
    """Agent exceeded retry limit."""
    pass