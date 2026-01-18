"""
LiteTask: A simple, lightweight, and versatile library for creating and managing tasks.

This library provides a straightforward interface for defining and executing tasks,
supporting both synchronous and asynchronous operations. It primarily focuses on
SQLite and in-memory storage for task management, aiming for minimal dependencies.
"""

from litetask.app import LiteTask

__version__ = "0.1.0"
__all__ = ["LiteTask"]
