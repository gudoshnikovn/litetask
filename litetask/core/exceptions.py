"""
Custom exception classes for LiteTask.

This module defines specific exception types used within the LiteTask library
to provide more granular error handling and clearer error messages.
"""


class LiteTaskError(Exception):
    """Base exception for all LiteTask-related errors."""


class BrokerError(LiteTaskError):
    """Exception raised when a broker operation fails."""


class TaskNotFoundError(LiteTaskError):
    """Exception raised when a requested task is not found in the registry."""


class WorkerError(LiteTaskError):
    """Exception raised when a worker encounters a critical error."""
