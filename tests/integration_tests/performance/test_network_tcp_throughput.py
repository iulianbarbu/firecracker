# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests the network throughput overhead added by Firecracker."""

import json
import logging
import time
import concurrent.futures
import re
import pytest
from conftest import _test_images_s3_bucket
from framework.artifacts import ArtifactCollection, ArtifactSet
from framework.matrix import TestMatrix, TestContext
from framework.builder import MicrovmBuilder, DEFAULT_HOST_IP
from framework.statistics import core, consumer, producer, criteria, \
    types, function
from framework.utils import CpuMap, ProcessCpuAffinity, CmdBuilder, run_cmd, \
    eager_map
import host_tools.network as net_tools



CONFIG = {
    "time": 20,  # seconds
    "modes": {
        "g2h": [""],
        "h2g": ["-R"],
        "bd": ["", "-R"]
    },
    "protocols": [
        {
            "name": "tcp",
            "omit": 5,  # seconds
            # Used as KBytes - socket buffer size.
            "window_size": [None, 256],
            "pkt_size": [None, 1024],
        }
    ],
    "baseline_bw": {  # Mbps
        "tcp-psDEFAULT-wsDEFAULT-2parallel-g2h": 2000,
        "tcp-psDEFAULT-ws256K-2parallel-g2h": 10500,
        "tcp-ps1024K-wsDEFAULT-2parallel-g2h": 13500,
        "tcp-ps1024K-ws256K-2parallel-g2h": 16500,
        "tcp-psDEFAULT-wsDEFAULT-2parallel-h2g": 16500,
        "tcp-psDEFAULT-ws256K-2parallel-h2g": 10500,
        "tcp-ps1024K-wsDEFAULT-2parallel-h2g": 13500,
        "tcp-ps1024K-ws256K-2parallel-h2g": 16500,
        "tcp-psDEFAULT-wsDEFAULT-1parallel-h2g": {
            "target": 21200,
            "delta": 500
        },
        "tcp-psDEFAULT-ws256K-1parallel-h2g": 10500,
        "tcp-ps1024K-wsDEFAULT-1parallel-h2g": 13500,
        "tcp-ps1024K-ws256K-1parallel-h2g": 16500,
        "tcp-psDEFAULT-wsDEFAULT-1parallel-g2h": {
                "target": 18000,
                "delta": 500
        },
        "tcp-psDEFAULT-ws256K-1parallel-g2h": 10500,
        "tcp-ps1024K-wsDEFAULT-1parallel-g2h": 13500,
        "tcp-ps1024K-ws256K-1parallel-g2h": 16500,
    },
    "baseline_guest_cpu_utilization": 30,
    # Percentage
    # "baseline_guest_cpu_utilization": {
    #     "tcp-ps128K-ws10K-1parallel-g2h": {"target": 8, "delta": 1},
    # }
}
IPERF3 = "iperf3"
THROUGHPUT = "throughput"
THROUGHPUT_TOTAL = "throughput_total"
DURATION = "duration"
DURATION_TOTAL = "duration_total"
RETRANSMITS = "retransmits"
RETRANSMITS_TOTAL = "retransmits_total"
CPU_UTILIZATION_HOST = "cpu_utilization_host"
CPU_UTILIZATION_HOST_TOTAL = "cpu_utilization_host_total"
CPU_UTILIZATION_GUEST = "cpu_utilization_guest"
CPU_UTILIZATION_GUEST_TOTAL = "cpu_utilization_guest_total"

BASE_PORT = 5000


def measurements_tcp():
    """Define the produced measurements for TCP workloads."""
    return [types.MeasurementDef(THROUGHPUT, "Mbps"),
            types.MeasurementDef(DURATION, "seconds"),
            types.MeasurementDef(RETRANSMITS, "#"),
            types.MeasurementDef(CPU_UTILIZATION_HOST, "percentage"),
            types.MeasurementDef(CPU_UTILIZATION_GUEST, "percentage")]


def stats_tcp(pipe_id: str):
    """Define statistics for TCP measurements."""
    # baseline_bw_target = CONFIG["baseline_bw"][pipe_id]["target"]
    # baseline_bw_delta = CONFIG["baseline_bw"][pipe_id]["delta"]

    baseline_guest_cpu_util = CONFIG["baseline_guest_cpu_utilization"]
    return [types.StatisticDef(THROUGHPUT_TOTAL, THROUGHPUT, function.Sum),
                               # criteria.EqualWith(baseline_bw_target, baseline_bw_delta)),
            types.StatisticDef(RETRANSMITS_TOTAL, RETRANSMITS, function.Sum),
            types.StatisticDef(DURATION_TOTAL, DURATION, function.Avg),
            types.StatisticDef(CPU_UTILIZATION_HOST_TOTAL,
                               CPU_UTILIZATION_HOST,
                               function.Sum),
            types.StatisticDef(CPU_UTILIZATION_GUEST_TOTAL,
                               CPU_UTILIZATION_GUEST,
                               function.Sum)]
                               # criteria.EqualWith(baseline_guest_cpu_util,
                               #                    10))]


def produce_iperf_output(basevm,
                         guest_cmd_builder,
                         cpu_affinity,
                         parallel,
                         modes):
    """Produce iperf raw output from server-client connection."""
    # Start the servers.
    rc, stdout, stderr = run_cmd(f"{basevm.jailer.netns_cmd_prefix()} ip a s")
    for pd in range(parallel):
        cpu_affinity = cpu_affinity + 1
        iperf_server = CmdBuilder("taskset") \
            .with_arg("--cpu-list", CpuMap(cpu_affinity)) \
            .with_arg(basevm.jailer.netns_cmd_prefix()) \
            .with_arg(IPERF3) \
            .with_arg("-sD") \
            .with_arg("-p", f"{BASE_PORT + pd}") \
            .with_arg("--one-off") \
            .build()
        run_cmd(iperf_server)
        # Wait for iperf3 servers to start.
        time.sleep(2)


    # Start `parallel` iperf3 clients. We can not use iperf3 parallel streams
    # due to non deterministic results and lack of scaling.
    def spawn_iperf_client(ssh_connection, base_port, determinant, mode):
        # Add the port where the iperf3 client is going to send/receive.
        cmd = guest_cmd_builder \
            .with_arg("-p", f"{base_port + determinant}") \
            .with_arg(mode) \
            .build()
        # pinned_cmd = f"taskset --cpu-list {determinant} {cmd}"
        rc, stdout, stderr = ssh_connection.execute_command(cmd,
                                                            without_asyncio=True)
        assert rc == 0
        assert stderr.read() == ""
        return stdout.read()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        ssh_connection = net_tools.SSHConnection(basevm.ssh_config)
        modes_len = len(modes)
        for pd in range(parallel):
            futures.append(executor.submit(spawn_iperf_client,
                                           ssh_connection,
                                           BASE_PORT,
                                           pd,
                                           # Distribute the modes evenly.
                                           modes[pd % modes_len]))
        for future in futures:
            res = future.result()
            yield res


def consume_iperf_tcp_output(cons, raw_output):
    """Consume iperf3 output result for TCP workload."""
    result = json.loads(raw_output)

    total_received = result['end']['sum_received']
    duration = float(total_received['seconds'])
    cons.consume_stat(DURATION_TOTAL, DURATION, duration)

    total_sent = result['end']['sum_sent']
    retransmits = int(total_sent['retransmits'])
    cons.consume_stat(RETRANSMITS_TOTAL, RETRANSMITS, retransmits)

    # Computed at the receiving end.
    bits_per_second = int(total_received['bits_per_second'])
    tput = round(bits_per_second / (1000**2), 2)
    cons.consume_stat(THROUGHPUT_TOTAL, THROUGHPUT, tput)

    cpu_util_host = result['end']['cpu_utilization_percent']['host_total']
    cpu_util_guest = result['end']['cpu_utilization_percent']['remote_total']
    cons.consume_stat(CPU_UTILIZATION_HOST_TOTAL,
                      CPU_UTILIZATION_HOST,
                      cpu_util_host)
    cons.consume_stat(CPU_UTILIZATION_GUEST_TOTAL,
                      CPU_UTILIZATION_GUEST,
                      cpu_util_guest)


def pipes(basevm, host_ip, parallel, cpu_affinity):
    """Pipes generator."""
    def generate_pipe(mode, proto, host_ip, parallel):
        for pkt_size in proto["pkt_size"]:
            for ws in proto["window_size"]:
                iperf_guest_cmd_builder = CmdBuilder(IPERF3) \
                    .with_arg("--verbose") \
                    .with_arg("--client", host_ip) \
                    .with_arg("--time", CONFIG["time"]) \
                    .with_arg("--json") \
                    .with_arg("--omit", proto["omit"])
                if ws:
                    ws_c=f"{ws}K"
                    iperf_guest_cmd_builder = iperf_guest_cmd_builder \
                        .with_arg("--window", ws_c)
                if pkt_size:
                    pkt_size_c=f"{pkt_size}K"
                    iperf_guest_cmd_builder = iperf_guest_cmd_builder \
                        .with_arg("--len", pkt_size_c)

                cons = consumer.LambdaConsumer(
                    consume_stats=True,
                    func=consume_iperf_tcp_output,
                )

                if not ws:
                    ws_c="DEFAULT"
                if not pkt_size:
                    pkt_size_c="DEFAULT"

                eager_map(cons.set_measurement_def, measurements_tcp())
                pipe_tag = f"tcp-ps{pkt_size_c}" \
                           f"-ws{ws_c}-{parallel}parallel-{mode}"
                eager_map(cons.set_stat_def, stats_tcp(pipe_tag))

                prod_kwargs = {
                    "guest_cmd_builder": iperf_guest_cmd_builder,
                    "basevm": basevm,
                    "cpu_affinity": cpu_affinity,
                    "parallel": parallel,
                    "modes": CONFIG["modes"][mode]
                }
                prod = producer.LambdaProducer(produce_iperf_output,
                                               prod_kwargs)
                yield cons, prod, pipe_tag

    for mode in CONFIG["modes"]:
        if mode == "bd" and parallel < 2:
            continue
        for proto in CONFIG["protocols"]:
            # Distribute modes evenly between producers and concumers.
            for cons, prod, pipe_tag in generate_pipe(mode,
                                                 proto,
                                                 host_ip,
                                                 parallel):
                yield cons, prod, pipe_tag


@pytest.mark.nonci
@pytest.mark.timeout(3600)
def test_network_throughput(network_config, bin_cloner_path):
    """Test network latency driver for multiple artifacts."""
    logger = logging.getLogger("network_tcp_throughput")
    artifacts = ArtifactCollection(_test_images_s3_bucket())
    microvm_artifacts = ArtifactSet(artifacts.microvms(keyword="2vcpu_1024mb"))
    microvm_artifacts.insert(artifacts.microvms(keyword="1vcpu_1024mb"))
    kernel_artifacts = ArtifactSet(artifacts.kernels())
    disk_artifacts = ArtifactSet(artifacts.disks(keyword="ubuntu"))

    # Create a test context and add builder, logger, network.
    test_context = TestContext()
    test_context.custom = {
        'builder': MicrovmBuilder(bin_cloner_path),
        'logger': logger,
        'name': 'network_tcp_throughput'
    }

    test_matrix = TestMatrix(context=test_context,
                             artifact_sets=[
                                 microvm_artifacts,
                                 kernel_artifacts,
                                 disk_artifacts
                             ])
    test_matrix.run_test(iperf_workload)


def iperf_workload(context):
    """Iperf between guest and host in both directions for TCP workload."""
    vm_builder = context.custom['builder']
    logger = context.custom["logger"]

    # Create a rw copy artifact.
    rw_disk = context.disk.copy()
    # Get ssh key from read-only artifact.
    ssh_key = context.disk.ssh_key()
    # Create a fresh microvm from aftifacts.
    basevm = vm_builder.build(kernel=context.kernel,
                              disks=[rw_disk],
                              ssh_key=ssh_key,
                              config=context.microvm)

    basevm.start()

    custom = {"microvm": context.microvm.name(),
              "kernel": context.kernel.name(),
              "disk": context.disk.name()}
    st_core = core.Core(name="network_tcp_throughput",
                        iterations=1,
                        custom=custom)

    match = re.match("([0-9]+)vcpu_[0-9]+mb", context.microvm.name())
    parallel = int(match.group(1))
    cpu_aff_curr = 0
    basevm_threads = ProcessCpuAffinity(basevm.jailer_clone_pid).get_threads()
    ProcessCpuAffinity(basevm_threads["fc_api"][0])\
        .set_cpu_affinity([cpu_aff_curr])
    cpu_aff_curr += 1
    ProcessCpuAffinity(basevm_threads["firecracker"][0])\
        .set_cpu_affinity([cpu_aff_curr])
    for i in range(parallel):
        cpu_aff_curr += 1
        ProcessCpuAffinity(basevm_threads[f"fc_vcpu {i}"][0])\
            .set_cpu_affinity([cpu_aff_curr])

    logger.info("Testing with microvm: \"{}\", kernel {}, disk {}"
                .format(context.microvm.name(),
                        context.kernel.name(),
                        context.disk.name()))

    for cons, prod, tag in \
            pipes(basevm,
                  DEFAULT_HOST_IP,
                  parallel,
                  cpu_aff_curr):
        st_core.add_pipe(prod, cons, tag)

    # Start running the commands on guest, gather results and verify pass
    # criteria.
    s = st_core.run_exercise()
    print(s)