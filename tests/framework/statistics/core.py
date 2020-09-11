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
    iterations: int
    results: dict
    custom: dict


Pipe = namedtuple("Pipe", "producer consumer")


class Core:
    """Base class for statistics core driver."""

    def __init__(self, name, iterations, custom={}):
        """Core constructor."""
        self._pipes = defaultdict(Pipe)
        self._statistics = Statistics(name=name,
                                      iterations=iterations,
                                      results={},
                                      custom=custom)

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
            result, custom = pipe.consumer.process()
            self._statistics['results'][tag] = result
            if len(custom) > 0:
                self._statistics['custom'][tag] = custom

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
    def iterations(self):
        """Return statistics iterations count."""
        return self._statistics.iterations

    @iterations.setter
    def iterations(self, iterations):
        """Set statistics iterations count."""
        self._statistics.iterations = iterations

    @property
    def custom(self):
        """Return statistics custom information."""
        return self._statistics.custom

    @iterations.setter
    def iterations(self, custom):
        """Set statistics custom information."""
        self._statistics.custom = custom

    @property
    def statistics(self):
        """Return statistics gathered so far."""
        return self._statistics
