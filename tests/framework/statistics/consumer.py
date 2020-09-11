# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Module for multiple statistics consumers."""


import re
from typing import Dict, List, Callable
from collections import namedtuple
from dataclasses import dataclass
# pylint: disable=E0611
from statistics import mean, stdev
from numbers import Number


class Method:
    """Default statistical methods."""

    @classmethod
    def last(cls, results):
        """Get the last result only."""
        return results[-1]

    @classmethod
    def min(cls, results):
        """Return the minimum result of the statistical exercise."""
        return min(results)

    @classmethod
    def max(cls, results):
        """Return the maximum result of the statistical exercise."""
        return max(results)

    @classmethod
    def avg(cls, results):
        """Return the average result of statistical exercise."""
        return mean(results)

    @classmethod
    def stddev(cls, results):
        """Return the stderr result so far into the statistical exercise."""
        assert len(results) > 0
        # pylint: disable=R0123
        if len(results) is 1:
            return results[0]

        return stdev(results)

    @classmethod
    def _percentile(cls, results, k):
        """Return the Kth percentile of the statistical exercise."""
        # pylint: disable=R0123
        if len(results) is 1:
            return results[0]

        length = len(results)
        results.sort()
        idx = length * k / 100
        if idx is not int(idx):
            return (results[int(idx)] + results[(int(idx) + 1)]) / 2

        return results[int(idx)]

    @classmethod
    def p50(cls, results):
        """Return the median."""
        return cls._percentile(results, 50)

    @classmethod
    def p90(cls, results):
        """Return the 90th percentile."""
        return cls._percentile(results, 90)

    @classmethod
    def p99(cls, results):
        """Return the 99th percentile."""
        return cls._percentile(results, 99)


class Criteria:
    """Comparison criteria between results and targets."""

    @classmethod
    # pylint: disable=C0103
    def gt(cls, target: Number):
        """Grater than comparision between numbers."""
        def result(actual: Number):
            return target < actual
        result.target = target
        result.name = "GreaterThan"
        return result

    @classmethod
    # pylint: disable=C0103
    def lt(cls, target: Number):
        """Lower than comparision between numbers."""
        def result(actual: Number):
            return target > actual
        result.target = target
        result.name = "LowerThan"
        return result

    @classmethod
    # pylint: disable=C0103
    def eq(cls, target: Number, tolerance: Number):
        """Equality with tolerance comparision between numbers."""
        def result(actual: Number):
            return abs(target - actual) <= tolerance
        result.target = str(target) + "(+-" + str(tolerance) + ")"
        result.name = "EqualWith"
        return result


MeasurementDef = namedtuple("MeasurementDefinition", "name unit")


@dataclass
class StatisticDef:
    """Statistic definition data class."""

    name: str
    measurement_name: str
    apply: Callable
    check: Criteria = None

    # pylint: disable=C0103
    MAX_KEY: str = "max"
    MIN_KEY: str = "min"
    AVG_KEY: str = "avg"
    STDDEV_KEY: str = "stddev"
    P50_KEY: str = "p50"
    P90_KEY: str = "p90"
    P99_KEY: str = "p99"
    # pylint: enable=C0103

    @classmethod
    def max(cls, measurement_name: str, criteria: Criteria):
        """Return max statistics definition."""
        return StatisticDef(StatisticDef.MAX_KEY, measurement_name, Method.max,
                            criteria)

    @classmethod
    def min(cls, measurement_name: str, criteria: Criteria):
        """Return min statistics definition."""
        return StatisticDef(StatisticDef.MIN_KEY, measurement_name, Method.min,
                            criteria)

    @classmethod
    def avg(cls, measurement_name, criteria):
        """Return average statistics definition."""
        return StatisticDef(StatisticDef.AVG_KEY, measurement_name, Method.avg,
                            criteria)

    @classmethod
    def stddev(cls, measurement_name: str, criteria: Criteria):
        """Return standard deviation statistics definition."""
        return StatisticDef(StatisticDef.STDDEV_KEY, measurement_name,
                            Method.stddev, criteria)

    @classmethod
    def p50(cls, measurement_name: str, criteria: Criteria):
        """Return 50 percentile statistics definition."""
        return StatisticDef(StatisticDef.P50_KEY, measurement_name, Method.p50,
                            criteria)

    @classmethod
    def p90(cls, measurement_name: str, criteria: Criteria):
        """Return 90 percentile statistics definition."""
        return StatisticDef(StatisticDef.P90_KEY, measurement_name, Method.p90,
                            criteria)

    @classmethod
    def p99(cls, measurement_name: str, criteria: Criteria):
        """Return 99 percentile statistics definition."""
        return StatisticDef(StatisticDef.P99_KEY, measurement_name, Method.p99,
                            criteria)

    @classmethod
    def defaults(cls,
                 measurement_name: str,
                 pass_criteria: dict) \
            -> List['StatisticDef']:
        """Return list with default statistics definitions."""
        default_stat_defs = []
        default_stat_defs.append(cls.max(measurement_name,
                                         pass_criteria.get(cls.MAX_KEY, None)))
        default_stat_defs.append(cls.min(measurement_name,
                                         pass_criteria.get(cls.MIN_KEY, None)))
        default_stat_defs.append(cls.avg(measurement_name,
                                         pass_criteria.get(cls.AVG_KEY, None)))
        default_stat_defs.append(cls.stddev(measurement_name,
                                            pass_criteria.get(cls.STDDEV_KEY,
                                                              None)))
        default_stat_defs.append(cls.p50(measurement_name,
                                         pass_criteria.get(cls.P50_KEY, None)))
        default_stat_defs.append(cls.p90(measurement_name,
                                         pass_criteria.get(cls.P90_KEY, None)))
        default_stat_defs.append(cls.p99(measurement_name,
                                         pass_criteria.get(cls.P99_KEY, None)))
        return default_stat_defs


class Consumer:
    """Base class for statistics aggregation class."""

    UNIT_KEY = "unit"
    RESULT_KEY = "result"

    # pylint: disable=W0102
    def __init__(self,
                 measurements_defs=[],
                 statistics_defs=[],
                 custom={},
                 consume_stats=False):
        """Initialize a consumer."""
        self._custom = {}
        self._results = {}  # Aggregated results.
        self._measurements_defs = {m.name: m for m in measurements_defs}
        self._statistics_defs = {s.name: s for s in statistics_defs}
        self._statistics = {}
        self._consume_stats = consume_stats

    def ingest(self, raw_data):
        """Abstract method for ingesting the raw result."""

    def consume_result(self, name, value):
        """Aggregate measurement."""
        results = self._results.get(name, None)
        if results is None:
            self._results[name] = []
        self._results[name].append(value)

    def consume_custom(self, name, value):
        self._custom[name] = value

    def process(self) -> Dict[str, dict]:
        """Generate statistics as a dictionary."""
        # Validate that all statistics have results data.
        assert len(self._results) is len(self._statistics_defs)
        for m_name in self._results:
            assert m_name in self._statistics_defs

        # Validate that all statistics have corresponding measurements data.
        for stat in self._statistics_defs.values():
            assert stat.measurement_name in self._measurements_defs

        # Generate consumer stats.
        for stat in self._statistics_defs.values():
            m_name = stat.measurement_name
            stat_unit = self._measurements_defs[m_name].unit
            self._statistics.setdefault(stat.name, {})[self.UNIT_KEY] \
                = stat_unit

            # We can either consume directly statistics, or compute them based
            # on measurements.
            if self._consume_stats:
                self._statistics[stat.name][self.RESULT_KEY] = \
                    stat.apply(self._results[stat.name])
            else:
                self._statistics[stat.name][self.RESULT_KEY] = \
                    stat.apply(self._results[m_name])

            # Check pass criteria.
            if callable(stat.check) and stat.check.target:
                res = self._statistics[stat.name][self.RESULT_KEY]
                assert stat.check(res), "%r criteria for %r failed." \
                                        " Target: %r vs actual: %r." \
                                        % (stat.check.name, stat.name,
                                           str(stat.check.target), str(res))
        return self._statistics, self._custom


class LambdaConsumer(Consumer):
    def __init__(self,
                 func,
                 measurements_defs=[],
                 statistics_defs=[],
                 consume_stats=False, *args):
        super().__init__(measurements_defs, statistics_defs, consume_stats)
        self.func = func
        self.args = args

    def ingest(self, raw_data):
        if not self.args:
            self.func(self, raw_data)
        else:
            self.func(self, raw_data, self.args)


class PingConsumer(Consumer):
    """Consumer for a specific `ping` tool command.

    Example command which generates a consumable output:
    ping -c {REQUESTS} -i {INTERVAL} {TARGET}
    """

    INT_REG = r'[0-9]+'
    FLOAT_REG = r'[+-]?[0-9]+\.[0-9]+'

    def __init__(self, requests, measurement_defs, statistic_defs, custom={}):
        """Initialize a PingConsumer."""
        self.requests = requests
        super().__init__(custom=custom,
                         measurements_defs=measurement_defs,
                         statistics_defs=statistic_defs,
                         consume_stats=True)

    def ingest(self, raw_data):
        """Ingest raw output of PING command as a formatted result.

        Example command which generates the output:
        ping -c {REQUESTS} -i {INTERVAL} {TARGET}
        """
        if raw_data is None or not raw_data:
            return

        stats = [StatisticDef.MIN_KEY, StatisticDef.AVG_KEY,
                 StatisticDef.MAX_KEY, StatisticDef.STDDEV_KEY]
        output = raw_data.split('\n')
        stat_values = output[-2]
        stat_values = re.findall(PingConsumer.FLOAT_REG, stat_values)
        for index, stat_value in enumerate(stat_values[:4]):
            self.consume_result(name=stats[index], value=float(stat_value))

        # Get statistics on packet loss.
        packet_stats = output[-3]
        packet_stats = packet_stats.split(',')[2]
        packet_stats = re.findall(PingConsumer.INT_REG, packet_stats)

        # Make sure we got only the packet loss percentage.
        assert len(packet_stats) == 1
        self.consume_result(name="pkt_loss", value=packet_stats[0])

        # Compute percentiles.
        seqs = output[1:self.requests + 1]
        times = []
        for index, seq in enumerate(seqs):
            time = re.findall(PingConsumer.FLOAT_REG + ' ms', seq)[0]
            time = re.findall(PingConsumer.FLOAT_REG, time)[0]
            times.append(time)

        times.sort()
        self.consume_result(name=StatisticDef.P50_KEY,
                            value=times[int(self.requests * 0.5)])
        self.consume_result(name=StatisticDef.P90_KEY,
                            value=times[int(self.requests * 0.9)])
        self.consume_result(name=StatisticDef.P99_KEY,
                            value=times[int(self.requests * 0.99)])
