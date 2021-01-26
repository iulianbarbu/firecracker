# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Module for common statistic tests metadata providers."""

from abc import ABC, abstractmethod
from framework.statistics.criteria import CriteriaFactory
from framework.statistics.function import FunctionFactory
from framework.statistics.types import MeasurementDef, StatisticDef
from framework.statistics.baseline import Provider as BaselineProvider


class Provider(ABC):
    """Backend for test metadata retrieval.

    Metadata consists from measurements and statistis definitions.
    """

    def __init__(self):
        self._measurements = dict()
        self._statistics = dict()

    @property
    @abstractmethod
    def measurements(self):
        """Return measurement dictionary."""

    @property
    @abstractmethod
    def statistics(self):
        """Return statistics dictionary."""


class DictProvider(Provider):
    """Backend for test metadata retrieval."""

    def __init__(self,
                 measurements: dict,
                 statistics: dict,
                 baseline_provider: BaselineProvider):

        """
        Initialize metadata provider.

        The provider expects to receive measurements following the below
        schema:
        ```
        "measurements": {
            "$id": "MEASUREMENTS_SCHEMA"
            "type": "object",
            "properties": {
                "key": {
                    "type": "string",
                    "description": "Measurement name."
                },
                "value": {
                    "type": "string",
                    "description": "Measurement unit."
                }
            }
        }
        ```

        The provider expects to receive statistics following the below schema:

        ```
        "statistics": {
            "$id": "STATISTICS_SCHEMA"
            "type": "object",
            "definitions": {
                "Criteria": {
                    "type": "string",
                    "description": "Comparison criteria class name. They are
                    implemented in the `statistics.criteria` module."
                }
                "Function": {
                    "type": "string",
                    "description": "Statistic functions class name. They are
                    implemented in the `statistics.function` module."
                }
                "StatisticDef": {
                    {
                        "type": "object",
                        "description": "Exhaustive statistic definition."
                        "properties": {
                            "name":     { "type": "string" },
                            "function": {
                                "type": "string"
                                "$ref": "#/definitions/Function"
                            },
                            "criteria": {
                                "type": "string"
                                "$ref" "#/definitions/Criteria"
                            }
                        },
                        "required": ["function"]
                    }
                }
            },
            "properties": {
                "key": {
                    "type": "string",
                    "description": "Measurement name."
                },
                "value": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/StatisticDef"
                    }
                }
        }
        """
        super().__init__()
        self._measurements = {key: MeasurementDef(key, measurements[
            key]) for key in measurements}

        for ms_name in statistics:
            assert ms_name in self._measurements, \
                f"'{ms_name}' can not be found in measurements definitions."

            self._statistics[ms_name] = dict()
            for stat_def in statistics[ms_name]:
                # Mandatory.
                func_cls_name = stat_def.get("function")
                assert func_cls_name, "'function' field is required for " \
                                      "statistic definition."

                func_cls = FunctionFactory.get(func_cls_name)
                assert func_cls, f"'{func_cls_name}' is not a valid " \
                                 f"statistic function."

                name = stat_def.get("name")
                func = func_cls()
                if name:
                    func = func_cls(name)

                criteria = None
                criteria_cls_name = stat_def.get("criteria")
                baseline = baseline_provider.get(ms_name, func.name)
                if criteria_cls_name and baseline:
                    criteria_cls = CriteriaFactory.get(criteria_cls_name)
                    assert criteria_cls, f"{criteria_cls_name} is not a " \
                                         f"valid criteria."
                    criteria = criteria_cls(baseline)

                self._statistics[ms_name][func.name] = StatisticDef(
                    measurement_name=ms_name,
                    func=func,
                    pass_criteria=criteria)

    @property
    def measurements(self):
        """Return measurement dictionary."""
        return self._measurements

    @property
    def statistics(self):
        """Return statistics dictionary."""
        return self._statistics
