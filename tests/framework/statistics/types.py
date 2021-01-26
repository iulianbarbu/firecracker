# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Module for common types definitions."""
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import List
from .criteria import ComparisonCriteria
from .function import Function


class DefaultMeasurement(Enum):
    """Default measurements."""

    CPU_UTILIZATION_VMM = 1
    CPU_UTILIZATION_VCPUS_TOTAL = 2


@dataclass
class MeasurementDef:
    """Measurement definition data class."""

    name: str
    unit: str

    @classmethod
    def cpu_utilization_vmm(cls):
        """Return vmm cpu utilization measurement definition."""
        return MeasurementDef(
            DefaultMeasurement.CPU_UTILIZATION_VMM.name.lower(),
            "percentage"
        )

    @classmethod
    def cpu_utilization_vcpus_total(cls):
        """Return vcpus total cpu utilization measurement definition."""
        return MeasurementDef(
            DefaultMeasurement.CPU_UTILIZATION_VCPUS_TOTAL.name.lower(),
            "percentage"
        )


@dataclass
class StatisticDef:
    """Statistic definition data class."""

    measurement_name: str
    func: Function
    pass_criteria: ComparisonCriteria = None

    @property
    def name(self) -> str:
        """Return the name used to identify the statistic definition."""
        return self.func.name

    @classmethod
    def defaults(cls,
                 measurement_name: str,
                 functions: List[Function],
                 pass_criteria: dict = None) -> List['StatisticDef']:
        """Return list with default statistics definitions.

        The expected `pass_criteria` dict is a dictionary with the following
        format:
        {
            # Statistic name explicitly provided in statistics definitions or
            # inherited from statistic functions (e.g Avg, Min, Max etc.).
            "key": str,
            # The comparison criteria used for pass/failure.
            "value": statistics.criteria.ComparisonCriteria,
        }
        """
        if pass_criteria is None:
            pass_criteria = defaultdict()
        else:
            pass_criteria = defaultdict(None, pass_criteria)

        default_stats = list()
        for function in functions:
            default_stats.append(
                StatisticDef(measurement_name=measurement_name,
                             func=function,
                             pass_criteria=pass_criteria.get(function.name)))
        return default_stats
