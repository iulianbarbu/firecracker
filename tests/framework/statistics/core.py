# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Consumer of statistics."""


from datetime import datetime
from collections import namedtuple, defaultdict
from typing_extensions import TypedDict
from framework.statistics.producer import Producer
from framework.statistics.consumer import Consumer


# pylint: disable=R0903
class Statistics(TypedDict):
    """Data class for aggregated statistic results."""

    name: str
    fc_version: str
    # TODO: instance, host linux kernel, guest linux kernel etc.
    platform: str
    iterations: int = 1
    custom: dict
    results: dict


Pipe = namedtuple("Pipe", "producer consumer")


class Core:
    """Base class for statistics core driver."""

    def __init__(self, name, fc_version, platform, iterations, custom):
        """Core constructor."""
        self._pipes = defaultdict(Pipe)
        self._statistics = Statistics(name=name,
                                      fc_version=fc_version,
                                      platform=platform,
                                      iterations=iterations,
                                      custom=custom,
                                      results={})

    def add_pipe(self, producer: Producer, consumer: Consumer, tag=None):
        """Add a new producer-consumer pipe."""
        if tag is None:
            tag = self._statistics['name'] + "_" + \
                str(datetime.timestamp(datetime.now()))
        self._pipes[tag] = Pipe(producer, consumer)

    def run_exercise(self) -> Statistics:
        """Drive the statistics producers until completion."""
        for pipe in self._pipes.values():
            iterations = self._statistics['iterations']
            while iterations:
                pipe.consumer.ingest(pipe.producer.produce())
                iterations -= 1

        for tag, pipe in self._pipes.items():
            result = pipe.consumer.generate_result()
            self._statistics['results'][tag] = result

        return self._statistics

    @property
    def name(self):
        """Return statistics name."""
        return self._statistics.name

    @name.setter
    def name(self, name):
        """Set statistics name."""
        self._statistics.name = name

    @property
    def fc_version(self):
        """Return statistics Firecracker version."""
        return self._statistics.fc_version

    @fc_version.setter
    def fc_version(self, fc_version):
        """Set statistics Firecracker version."""
        self._statistics.fc_version = fc_version

    @property
    def platform(self):
        """Return statistics platform."""
        return self._statistics.platform

    @platform.setter
    def platform(self, platform):
        """Set statistics platform."""
        self._statistics.platform = platform

    @property
    def iterations(self):
        """Return statistics iterations count."""
        return self._statistics.iterations

    @iterations.setter
    def iterations(self, iterations):
        """Set statistics iterations count."""
        self._statistics.iterations = iterations

    @property
    def statistics(self):
        """Return statistics gathered so far."""
        return self._statistics
