# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Single threaded producer/consumer for statistics gathering.

The purpose of this module is to provide primitives for statistics exercises
which need a common framework that sets expectations in terms of tests
design and results.

The main components of the module consist from: `Core`, `Producer` and
`Consumer`. The `Core` is the component which drives the interaction between
`Producer` and `Consumer`. The `Producer` goal is to pass raw data to the
`Consumer`, while the `Consumer` is responsible for raw data processing and
transformation.

First example:
```
import json
from numbers import Number
from random import randint
from framework.statistics.metadata import DictProvider as DictMetadataProvider
from framework.utils import DictQuery
import framework.statistics.criteria as criteria
from framework.statistics.baseline import Provider as BaselineProvider
from framework.statistics.core import Core
from framework.statistics.producer import LambdaProducer
from framework.statistics.consumer import LambdaConsumer

CONFIG = {
    "TEST_ID": "random-ints-gen",
    "iterations": 10,
    "lower_bound": 0,
    "upper_bound": 99,
    "measurements": {
        "pipe1": {
            "integers": "none",
        },
        "pipe2": {
            "integer": "none"
        }
    },
    "statistics": {
        "pipe1": {
            "integers": [{
                "function": "Sum",
                "criteria": "LowerThan",
            }]
        },
        "pipe2": {
            "integer": [{
                "function": "ValuePlaceholder",
                "criteria": "GreaterThan",
            }]
        }
    },
    "baselines": {
        "pipe1": {
            "integers": {
                "rand": {
                    "Sum": {
                        "target": 100,
                    }
                }
            }
        },
        "pipe2": {
            "integer": {
                "rand": {
                    "value": {
                        "target": 50,
                    }
                }
            }
        }
    }
}

class DummyBaselineProvider(BaselineProvider):
    def __init__(self, pipe_id, env_id):
        super().__init__(DictQuery(dict()))
        if "baselines" in CONFIG:
            super().__init__(DictQuery(CONFIG["baselines"][pipe_id]))

        self._tag = "{}/" + env_id + "/{}"

    def get(self, ms_name: str, st_name: str) -> dict:
        key = self._tag.format(ms_name, st_name)
        baseline = self._baselines.get(key)
        if baseline:
            target = baseline.get("target")
            return {
                "target": target,
            }
        return None

st_core = Core(name=CONFIG["TEST_ID"], iterations=CONFIG["iterations"])
# Define the producer.
st_prod = LambdaProducer(lambda: randint(0, 99))

# Define the `Consumer`
consumer_func1 = lambda cons, res: cons.consume_measurement("integers", res)
consumer_func2 = lambda cons, res: cons.consume_stat("value", "integer", res)
st_cons1 = LambdaConsumer(
        consumer_func1,
        metadata_provider=DictMetadataProvider(
                CONFIG["measurements"]["pipe1"],
                CONFIG["statistics"]["pipe1"],
                DummyBaselineProvider("pipe1", env_id="rand")))
st_cons2 = LambdaConsumer(
        consumer_func2,
        metadata_provider=DictMetadataProvider(
                CONFIG["measurements"]["pipe2"],
                CONFIG["statistics"]["pipe2"],
                DummyBaselineProvider("pipe2", env_id="rand")))

# Add Producer/Consumer pipes.
st_core.add_pipe(st_prod, st_cons1, tag="pipe1")
st_core.add_pipe(st_prod, st_cons2, tag="pipe2")

# Start the exercise without checking the criteria.
st_core.run_exercise(check_criteria=False)
```

Output:
```
{
    'name': 'random-ints-gen',
    'iterations': 10,
    'results': {
        'pipe1': {
            'integers': {
                '_unit': 'none',
                'Sum': 566}
            },
        'pipe2': {
            'integer': {
                '_unit': 'none',
                'value': 43}
            }
        },
        'custom': {}
    }
}
```

Second example, similar to the first, but using a more intrusive way of
defining statistics and measurements:
```
import json
from numbers import Number
from random import randint
from framework.statistics.metadata import DictProvider as DictMetadataProvider
import framework.statistics.criteria as criteria
from framework.statistics.core import Core
from framework.utils import DictQuery, eager_map
from framework.statistics.producer import LambdaProducer
from framework.statistics.consumer import LambdaConsumer
from framework.statistics.types import MeasurementDef, StatisticDef
from framework.statistics.function import FunctionFactory

CONFIG = {
    "TEST_ID": "random-ints-gen",
    "iterations": 10,
    "lower_bound": 0,
    "upper_bound": 99,
    "measurements": {
        "pipe1": {
            "integers": "none",
        },
        "pipe2": {
            "integer": "none"
        }
    },
    "statistics": {
        "pipe1": {
            "integers": [{
                "function": "Sum",
                "criteria": "LowerThan",
            }]
        },
        "pipe2": {
            "integer": [{
                "function": "ValuePlaceholder",
                "criteria": "GreaterThan",
            }]
        }
    },
    "baselines": {
        "pipe1": {
            "integers": {
                "rand": {
                    "Sum": {
                        "target": 100,
                    }
                }
            }
        },
        "pipe2": {
            "integer": {
                "rand": {
                    "value": {
                        "target": 50,
                    }
                }
            }
        }
    }
}

def baseline(ms_name: str, st_name: str, pipe_id: str):
    baselines = DictQuery(CONFIG["baselines"][pipe_id])
    target = baselines.get(f"{ms_name}/rand/{st_name}/target")
    return {
        "target": target
    }

def measurements(pipe_id: str):
    ms_list = list()
    for ms_name in CONFIG["measurements"][pipe_id]:
        ms_list.append(
            MeasurementDef(ms_name, CONFIG["measurements"][pipe_id][ms_name])
        )
    return ms_list

def stats(pipe_id: str):
    st_list = list()
    for ms_name in CONFIG["statistics"][pipe_id]:
        for st_def in CONFIG["statistics"][pipe_id][ms_name]:
            func_cls_id = st_def["function"]
            func_cls = FunctionFactory.get(func_cls_id)
            criteria_cls_id = st_def["criteria"]
            criteria_cls = criteria.CriteriaFactory.get(criteria_cls_id)
            st_list.append(
                StatisticDef(
                    ms_name,
                    func_cls(),
                    criteria_cls(baseline(ms_name, func_cls.__name__, pipe_id))
                )
            )
    return st_list

# Define the core. By using `check=False` parameter we will tell it to
# bypass comparison criteria and print the result to stdout.
st_core = Core(name=CONFIG["TEST_ID"],
               iterations=CONFIG["iterations"])
# Define the producer.
st_prod = LambdaProducer(lambda: randint(0, 99))

# Define the `Consumer`
consumer_func1 = lambda cons, res: cons.consume_measurement("integers", res)
consumer_func2 = lambda cons, res: cons.consume_stat("value", "integer", res)
st_cons1 = LambdaConsumer(consumer_func1)
eager_map(st_cons1.set_measurement_def, measurements("pipe1"))
eager_map(st_cons1.set_stat_def, stats("pipe1"))
st_cons2 = LambdaConsumer(consumer_func2)
eager_map(st_cons2.set_measurement_def, measurements("pipe2"))
eager_map(st_cons2.set_stat_def, stats("pipe2"))

# Add Producer/Consumer pipes.
st_core.add_pipe(st_prod, st_cons1, tag="pipe1")
st_core.add_pipe(st_prod, st_cons2, tag="pipe2")

# Start the exercise without checking the criteria.
st_core.run_exercise(False)
```

Output:
```
{
    'name': 'random-ints-gen',
    'iterations': 10,
    'results': {
        'pipe1': {
            'integers': {'_unit': 'none', 'Sum': 454}
        },
        'pipe2': {
            'integer': {'_unit': 'none', 'value': 12}
        }
    },
    'custom': {}
}
```
"""

from . import core
from . import consumer
from . import producer
from . import types
from . import criteria
from . import function
from . import baseline
from . import metadata
