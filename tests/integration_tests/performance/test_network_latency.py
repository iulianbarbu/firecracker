# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests the network latency of a Firecracker guest."""

import logging
import platform
import pytest
import host_tools.network as net_tools
from conftest import _test_images_s3_bucket
from framework.artifacts import ArtifactCollection, ArtifactSet
from framework.matrix import TestMatrix, TestContext
from framework.builder import MicrovmBuilder
from framework.statistics import core, consumer, producer
from framework import utils


PING = "ping -c {} -i {} {}"
BASELINES = {
    "x86_64": {
        "target": 0.15,
        "delta": 0.05
    },
    "aarch64": {
        "target": 0.110,
        "delta": 0.015
    }
}

@pytest.mark.timeout(900)
@pytest.mark.nonci
def test_network_latency(network_config, bin_cloner_path):
    """Test network latency driver for multiple artifacts."""
    logger = logging.getLogger("snapshot_sequence")
    artifacts = ArtifactCollection(_test_images_s3_bucket())
    # Testing matrix:
    # - Guest kernel: Linux 4.9/4.14
    # - Rootfs: Ubuntu 18.04
    # - Microvm: 2vCPU with 1024 MB RAM
    microvm_artifacts = ArtifactSet(artifacts.microvms(keyword="2vcpu_1024mb"))
    kernel_artifacts = ArtifactSet(artifacts.kernels())
    disk_artifacts = ArtifactSet(artifacts.disks(keyword="ubuntu"))

    # Create a test context and add builder, logger, network.
    test_context = TestContext()
    test_context.custom = {
        'builder': MicrovmBuilder(bin_cloner_path),
        'network_config': network_config,
        'logger': logger,
        'requests': 1000,
        'interval': 0.2,  # Seconds.
        'name': 'network_latency'
    }

    # Create the test matrix.
    test_matrix = TestMatrix(context=test_context,
                             artifact_sets=[
                                 microvm_artifacts,
                                 kernel_artifacts,
                                 disk_artifacts
                             ])

    test_matrix.run_test(_g2h_send_ping)


def _g2h_send_ping(context):
    """Send ping from guest to host."""
    logger = context.custom['logger']
    vm_builder = context.custom['builder']
    interval_between_req = context.custom['interval']
    network_config = context.custom['network_config']
    name = context.custom['name']

    logger.info("Testing {} with microvm: \"{}\", kernel {}, disk {} "
                .format(name,
                        context.microvm.name(),
                        context.kernel.name(),
                        context.disk.name()))

    # Create a rw copy artifact.
    rw_disk = context.disk.copy()
    # Get ssh key from read-only artifact.
    ssh_key = context.disk.ssh_key()
    # Create a fresh microvm from aftifacts.
    basevm = vm_builder.build(kernel=context.kernel,
                              disks=[rw_disk],
                              ssh_key=ssh_key,
                              config=context.microvm,
                              network_config=network_config)

    _tap, host_ip, _ = basevm.ssh_network_config(network_config, '1')

    basevm.start()
    fc_pid = basevm.jailer_clone_pid
    threads = utils.get_threads(fc_pid)
    utils.set_cpu_affinity(threads['firecracker'], [0])
    utils.set_cpu_affinity(threads['fc_api'], [1])
    utils.set_cpu_affinity(threads['fc_vcpu 0'], [2])
    utils.set_cpu_affinity(threads['fc_vcpu 1'], [3])

    ssh_connection = net_tools.SSHConnection(basevm.ssh_config)
    custom = {"microvm": context.microvm.name(),
              "kernel": context.kernel.name(),
              "disk": context.disk.name()}
    st_core = core.Core(name="network_latency", iterations=1, custom=custom)

    # Measurements.
    pkt_loss = "pkt_loss"
    latency = "latency"

    # Pass criteria baselines.
    delta = BASELINES[platform.machine()]["delta"]
    target = BASELINES[platform.machine()]["target"]

    # Define measurements.
    measurement_defs = [consumer.MeasurementDef(latency, "millisecond")]
    measurement_defs.append(consumer.MeasurementDef(pkt_loss, "percentage"))

    # Add a pass criteria for AVG.
    criteria = {}
    criteria[consumer.StatisticDef.AVG_KEY] = consumer.Criteria.eq(target,
                                                                   delta)

    # Define statistics for the measurements.
    stats_defs = consumer.StatisticDef.defaults(latency,
                                                pass_criteria=criteria)
    pkt_loss_stat = consumer.StatisticDef(pkt_loss, pkt_loss,
                                          consumer.Method.last)
    stats_defs.append(pkt_loss_stat)

    requests = context.custom['requests']
    cons = consumer.PingConsumer(requests, measurement_defs, stats_defs, custom)
    prod = producer.SSHCommand(PING.format(requests,
                                           interval_between_req,
                                           host_ip),
                               ssh_connection=ssh_connection)
    st_core.add_pipe(producer=prod, consumer=cons)

    # Start gathering the results.
    s = st_core.run_exercise()
    print(s)
