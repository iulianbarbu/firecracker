# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Module for comparison criteria."""

from abc import ABC, abstractmethod
from pydoc import locate


class Failed(Exception):
    """Exception to be raised when criteria fails."""

    def __init__(self, msg=""):
        """Initialize the exception."""
        super().__init__()
        self._msg = msg

    @property
    def msg(self):
        """Return the exception message."""
        return self._msg

    @msg.setter
    def msg(self, msg):
        """Set the exception message."""
        self._msg = msg


class CriteriaFactory:
    """Comparison criteria factory class."""

    @classmethod
    def get(cls, criteria_cls_name) -> 'ComparisonCriteria':
        """`criteria_cls_name` must be a valid criteria class name."""
        return locate(f"framework.statistics.criteria.{criteria_cls_name}")


# pylint: disable=R0903
class ComparisonCriteria(ABC):
    """Comparison criteria between results and targets."""

    def __init__(self, name: str, baseline: dict):
        """Initialize the comparison criteria.

        Baseline expected schema:
        ```
        {
            "type": "object",
            "properties": {
                "target": number,
            },
            "required": ["target"]
        }
        ```
        """
        self._baseline = baseline
        self._name = name

    @abstractmethod
    def check(self, actual):
        """Compare the target and the actual."""

    @property
    def name(self):
        return self._name

    @property
    def target(self):
        target = self._baseline.get("target")
        if not target:
            raise Failed("Baseline target not defined.")

        return target


# pylint: disable=R0903
class GreaterThan(ComparisonCriteria):
    """Greater than comparison criteria."""

    def __init__(self, baseline: dict):
        """Initialize the criteria."""
        super().__init__("GreaterThan", baseline)

    def check(self, actual):
        """Compare the target and the actual."""
        fail_msg = self.name + f" failed. Target: '{self.target} " \
                               f"vs Actual: '{actual}'."
        if actual < self.target:
            raise Failed(msg=fail_msg)


# pylint: disable=R0903
class LowerThan(ComparisonCriteria):
    """Lower than comparison criteria."""

    def __init__(self, baseline: dict):
        """Initialize the criteria."""
        super().__init__("LowerThan", baseline)

    def check(self, actual):
        """Compare the target and the actual."""
        fail_msg = self.name + f" failed. Target: '{self.target} " \
                               f"vs Actual: '{actual}'."
        if actual > self.target:
            raise Failed(msg=fail_msg)


# pylint: disable=R0903
class EqualWith(ComparisonCriteria):
    """Equal with comparison criteria. Baseline expected schema:
    ```
    {
        "type": "object",
        "properties": {
            "target": number,
            "delta": number
        },
        "required": ["target", "delta"]
    }
    ```
    """

    def __init__(self, baseline: dict):
        """Initialize the criteria."""
        super().__init__("EqualWith", baseline)

    @property
    def delta(self):
        """Return the `delta` field of the baseline."""
        delta = self._baseline.get("delta")
        if not delta:
            raise Failed("Baseline delta not defined.")
        return delta

    def check(self, actual):
        """Compare the target and the actual."""
        fail_msg = self.name + f" failed. Target: '{self.target} +- " \
                               f"{self.delta}' vs Actual: '{actual}'."
        if abs(self.target - actual) > self.delta:
            raise Failed(msg=fail_msg)
