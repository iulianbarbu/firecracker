# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Single threaded producer/consumer for statistics gathering.

The purpose of this module is to provide primitives for statistics exercises
which need a common framework that sets expectations in terms of tests
design and results.

The main components of the module consist from: `Core`, `Producer` and
`Consumer`, `ComparisonCriteria`, metadata providers and baselines providers.

The `Core` is the component which drives the interaction between `Producer`
and `Consumer`. The `Producer` goal is to pass raw data to the `Consumer`,
while the `Consumer` is responsible for raw data processing and transformation.
Metadata and baselines providers are independently used by the `Consumer` to
get measurements and statistics definitions relevant in the processing and
transformation step. In the end, the processing and transformation step
makes use of comparison criteria, present in statistics definitions,
which will assert expectations in terms of exercise end result.

Lets create a test using the above components. The test will answer to two
questions:
1. What is the sum of 10 randomly generated integers, between 0 and 100,
fetched with `randint` module?
2. What is the 10th randomly generated integer, between 0 and 100, fetched with
`randint` module?

We can define two exercises from the above questions, so lets call them
`10RandomIntsSumExercise` and `10thRandomIntExercise`. The test logic starts
with defining raw data producers for both exercises. The producer definition
depends on the chosen implementation. We're going to use the `LambdaProducer`.
This producer needs a function which produces the data.

```
from random import randint
from framework.statistics.producer import LambdaProducer

st_prod_func = lambda llimit, ulimit: randint(llimit, ulimit)
st_prod = LambdaProducer(
        func=st_prod_func,
        func_kwargs={"llimit": 0, "ulimit": 99}
)
```

Next up, we need to define consumers for the `st_prod`. For the
`10RandomIntsSumExercise`, the consumer must process 10 random integers and
sum them up, while for the `10thRandomIntExercise`, the consumer must process
the 10th random generated integer and return it. `Consumer`s definitions
depend largely on the chosen consumer implementations. We're going to use the
`LambdaConsumer`. To define a `LambdaConsumer` we need the following resources:

 1. Measurements definitions: provided through metadata and baselines providers
 or through the `Consumer`s `set_measurement_def` interface. They can be
 hardcoded in the test logic or programmatically generated. We're going to
 use here the programmatic alternative, where measurements definitions
 will be found in a global config dictionary, processed through programmatic
 means.
 2. A function that processes and transforms the data coming from the
 `st_prod`.

Let's lay down our measurements definitions first inside the test global
configuration dictionary. The dictionary consists from measurements
definitions and from baselines, which are going to be used for setting up
pass criteria for measurements statistics.
```
CONFIG = {
    "measurements": {
        # This is a map where keys represent the exercise id while the
        # values represent a map from measurements name to measurements
        # definition. The values follow the expected `DictProvider` schema.
        "10RandomIntsSumExercise": {
            "ints": { # Measurement name.
                "unit": "none", # We do not have a specific measurement unit.
                "statistics": [
                    {
                        # By default, the statistic definition name is the
                        # function name.
                        "function": "Sum",
                        "criteria": "LowerThan"
                    }
                ]
            }
        },
        "10thRandomIntExercise": {
            "int": {
                "unit": "none", # We do not have a specific measurement unit.
                "statistics": [
                    {
                        "function": "ValuePlaceholder",
                        "criteria": "GreaterThan",
                    }
                ]
            }
        }
    },
    "baselines": {
        "10RandomIntsSumExercise": {
            "ints": {
                # Info about the environment that generated the data.
                "randint": {
                    "Sum": {
                        "target": 600,
                    }
                }
            }
        },
        "10thRandomIntExercise": {
            "int": {
                "randint": {
                    "value": {
                        "target": 50,
                    }
                }
            }
        }
    }
}
```

We'll continue by implementing the metadata and baseline providers. The
measurements definitions from the global configuration dictionary
will be processed by the `DictProvider` metadata provider. The measurements
definitions schema can be found in the `DictProvider` documentation.
```
from framework.statistics.metadata import DictProvider as DictMetadataProvider
from framework.statistics.baseline import Provider as BaselineProvider
from framework.utils import DictQuery

# The baseline provider is a requirement for the `DictProvider`.
class RandintBaselineProvider(BaselineProvider):
    def __init__(self, exercise_id, env_id):
        super().__init__(DictQuery(dict()))
        if "baselines" in CONFIG:
            super().__init__(DictQuery(CONFIG["baselines"][exercise_id]))
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

baseline_provider_sum = RandintBaselineProvider(
    "10RandomIntsSumExercise",
    "randint")
baseline_provider_10th = RandintBaselineProvider(
    "10thRandomIntExercise",
    "randint")

metadata_provider_sum = DictMetadataProvider(
    CONFIG["measurements"]["10RandomIntsSumExercise"],
    baseline_provider_sum)
metadata_provider_10th = DictMetadataProvider(
    CONFIG["measurements"]["10thRandomIntExercise"],
    baseline_provider_10th)
```

The alternative here would be to manually define our measurements and pass
them to the `LambdaConsumer` at a later step. Depending on the magnitude of
the exercise, this alternative might be preffered over the other. Here's how
it can be done:
```
from framework.utils import DictQuery
from framework.statistics.function import FunctionFactory
from framework.statistics.criteria import CriteriaFactory
from framework.statistics.types import MeasurementDef, StatisticDef

def baseline(ms_name: str, st_name: str, exercise_id: str):
    baselines = DictQuery(CONFIG["baselines"][exercise_id])
    target = baselines.get(f"{ms_name}/randint/{st_name}/target")
    return {
        "target": target
    }

def measurements(exercise_id: str):
    ms_list = list()
    for ms_name in CONFIG["measurements"][exercise_id]:
        st_list = list()
        unit = CONFIG["measurements"][exercise_id][ms_name]["unit"]
        st_defs = CONFIG["measurements"][exercise_id][ms_name]["statistics"]
        for st_def in st_defs:
            func_cls_id = st_def["function"]
            func_cls = FunctionFactory.get(func_cls_id)
            criteria_cls_id = st_def["criteria"]
            criteria_cls = CriteriaFactory.get(criteria_cls_id)
            bs = baseline(ms_name, func_cls.__name__, exercise_id)
            st_list.append(StatisticDef(func_cls(), criteria_cls(bs)))
        ms_list.append(MeasurementDef(ms_name, unit, st_list))
    return ms_list
```

Next, having our measurements definitions layed out, we can start defining
`LambdaConsumer`s functions. The functions are strictly related to
`LambdaProducer` function, so in our case we need to process an integer
coming from the producer.
```
# The following function is consuming data points, pertaining to measurements
# defined above.
st_cons_sum_func = lambda cons, res: cons.consume_data("ints", res)

# Here we consume a statistic value directly. Statistics can be both consumed
# or computed based on their measurement data points, consumed via the
# `Consumer`s `consume_data` interface.
st_cons_10th_func = lambda cons, res: cons.consume_stat("value", "int", res)
```

We can define now our `LambdaConsumer`s for both exercices:

1. Through the metadata and baseline providers.
```
from framework.statistics.consumer import LambdaConsumer

st_cons_sum = LambdaConsumer(
        st_cons_sum_func,
        metadata_provider=metadata_provider_sum)
st_cons_10th = LambdaConsumer(
        st_cons_10th_func,
        metadata_provider=metadata_provider_10th)
```

2. By setting the measurements definitions separately:
```
from framework.statistics.consumer import LambdaConsumer
from framework.utils import eager_map

st_cons_sum = LambdaConsumer(st_cons_sum_func)
eager_map(st_cons_sum.set_measurement_def, measurements("10RandomIntsSumExercise"))
st_cons_10th = LambdaConsumer(st_cons_10th_func)
eager_map(st_cons_10th.set_measurement_def, measurements("10thRandomIntExercise"))
```

Once we have defined our producers and consumers, we will continue by
defining the statistics `Core`.
```
from framework.statistics.core import Core

# Both exercises require the core to drive both producers and consumers for
# 10 iterations to achieve the wanted result.
st_core = Core(name="randint_observation", iterations=10)
st_core.add_pipe(st_prod, st_cons_sum, tag="10RandomIntsSumExercise")
st_core.add_pipe(st_prod, st_cons_10th, tag="10thRandomIntExercise")
```

Let's start the exercise without verifying the criteria:
```
# Start the exercise without checking the criteria.
st_core.run_exercise(check_criteria=False)
```

Output:
```
{
    'name': 'randint_observation',
    'iterations': 10,
    'results': {
        '10RandomIntsSumExercise': {
            'ints': {
                '_unit': 'none',
                'Sum': 454
            }
        },
        '10thRandomIntExercise': {
            '10thRandomIntExercise': {
                '_unit': 'none',
                'value': 12
            }
        }
    },
    'custom': {}
}
```

Now, verifying the criteria:
```
# Start the exercise without checking the criteria.
st_core.run_exercise()
```

Output for failure on `10RandomIntsSumExercise`:
```
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/Users/iul/iul_fc/tests/framework/statistics/core.py", line 63,
        in run_exercise
    assert False, f"Failed on '{tag}': {err.msg}"
AssertionError: Failed on '10RandomIntsSumExercise': 'ints/Sum':
    LowerThan failed. Target: '100 vs Actual: '3095'.
```

Output for failure on `10thRandomIntExercise`:
```
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/Users/iul/iul_fc/tests/framework/statistics/core.py", line 63,
        in run_exercise
    assert False, f"Failed on '{tag}': {err.msg}"
AssertionError: Failed on '10thRandomIntExercise': 'int/value': GreaterThan
    failed. Target: '50 vs Actual: '42'.
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
