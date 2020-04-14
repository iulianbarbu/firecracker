# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests that verify MMDS related functionality."""

import host_tools.network as net_tools


def _assert_out(stdout, stderr, expected):
    assert stderr.read() == ''
    assert stdout.read() == expected


def test_custom_ipv4(test_microvm_with_ssh, network_config):
    """Test the API for MMDS custom ipv4 support."""
    test_microvm = test_microvm_with_ssh
    test_microvm.spawn()

    response = test_microvm.mmds.get()
    assert test_microvm.api_session.is_status_ok(response.status_code)
    assert response.json() == {}

    data_store = {
        'latest': {
            'meta-data': {
                'ami-id': 'ami-12345678',
                'reservation-id': 'r-fea54097',
                'local-hostname': 'ip-10-251-50-12.ec2.internal',
                'public-hostname': 'ec2-203-0-113-25.compute-1.amazonaws.com',
                'network': {
                    'interfaces': {
                        'macs': {
                            '02:29:96:8f:6a:2d': {
                                'device-number': '13345342',
                                'local-hostname': 'localhost',
                                'subnet-id': 'subnet-be9b61d'
                            }
                        }
                    }
                }
            }
        }
    }
    response = test_microvm.mmds.put(json=data_store)
    assert test_microvm.api_session.is_status_no_content(response.status_code)

    response = test_microvm.mmds.get()
    assert test_microvm.api_session.is_status_ok(response.status_code)
    assert response.json() == data_store

    config_data = {
        'ipv4_address': ''
    }
    response = test_microvm.mmds.put_config(json=config_data)
    assert test_microvm.api_session.is_status_bad_request(response.status_code)

    config_data = {
        'ipv4_address': '169.254.169.250'
    }
    response = test_microvm.mmds.put_config(json=config_data)
    assert test_microvm.api_session.is_status_no_content(response.status_code)

    test_microvm.basic_config(vcpu_count=1)
    _tap = test_microvm.ssh_network_config(
         network_config,
         '1',
         allow_mmds_requests=True
    )

    test_microvm.start()
    ssh_connection = net_tools.SSHConnection(test_microvm.ssh_config)

    response = test_microvm.mmds.put_config(json=config_data)
    assert test_microvm.api_session.is_status_bad_request(response.status_code)

    cmd = 'ip route add 169.254.169.250 dev eth0'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '')

    pre = 'curl -s http://169.254.169.250/'

    cmd = pre + 'latest/meta-data/ami-id'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '"ami-12345678"')

    # The request is still valid if we append a
    # trailing slash to a leaf node.
    cmd = pre + 'latest/meta-data/ami-id/'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '"ami-12345678"')

    cmd = pre + 'latest/meta-data/network/interfaces/macs/'\
        '02:29:96:8f:6a:2d/subnet-id'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '"subnet-be9b61d"')

    # Test reading a non-leaf node WITHOUT a trailing slash.
    cmd = pre + 'latest/meta-data'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(
        stdout,
        stderr,
        '{"ami-id":"ami-12345678","local-hostname":\
"ip-10-251-50-12.ec2.internal","network":{"interfaces":\
{"macs":{"02:29:96:8f:6a:2d":{"device-number":"13345342",\
"local-hostname":"localhost","subnet-id":"subnet-be9b61d"}}}},\
"public-hostname":"ec2-203-0-113-25.compute-1.amazonaws.com",\
"reservation-id":"r-fea54097"}')

    # Test reading a non-leaf node with a trailing slash.
    cmd = pre + 'latest/meta-data/'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(
        stdout,
        stderr,
        '{"ami-id":"ami-12345678","local-hostname":\
"ip-10-251-50-12.ec2.internal","network":{"interfaces":\
{"macs":{"02:29:96:8f:6a:2d":{"device-number":"13345342",\
"local-hostname":"localhost","subnet-id":"subnet-be9b61d"}}}},\
"public-hostname":"ec2-203-0-113-25.compute-1.amazonaws.com",\
"reservation-id":"r-fea54097"}')


def test_json_response(test_microvm_with_ssh, network_config):
    """Test the MMDS json response."""
    test_microvm = test_microvm_with_ssh
    test_microvm.spawn()

    response = test_microvm.mmds.get()
    assert test_microvm.api_session.is_status_ok(response.status_code)
    assert response.json() == {}

    data_store = {
        'latest': {
            'meta-data': {
                'ami-id': 'ami-12345678',
                'reservation-id': 'r-fea54097',
                'local-hostname': 'ip-10-251-50-12.ec2.internal',
                'public-hostname': 'ec2-203-0-113-25.compute-1.amazonaws.com',
                'dummy_res': ['res1', 'res2']
            },
            "Limits": {
                "CPU": 512,
                "Memory": 512
            },
            "Usage": {
                "CPU": 12.12
            }
        }
    }
    response = test_microvm.mmds.put(json=data_store)
    assert test_microvm.api_session.is_status_no_content(response.status_code)

    response = test_microvm.mmds.get()
    assert test_microvm.api_session.is_status_ok(response.status_code)
    assert response.json() == data_store

    test_microvm.basic_config(vcpu_count=1)
    _tap = test_microvm.ssh_network_config(
         network_config,
         '1',
         allow_mmds_requests=True
    )

    test_microvm.start()
    ssh_connection = net_tools.SSHConnection(test_microvm.ssh_config)

    cmd = 'ip route add 169.254.169.254 dev eth0'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '')

    pre = 'curl -s http://169.254.169.254/'

    cmd = pre + 'latest/meta-data/'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '{"ami-id":\
"ami-12345678","dummy_res":["res1","res2"],"local-hostname":\
"ip-10-251-50-12.ec2.internal","public-hostname":\
"ec2-203-0-113-25.compute-1.amazonaws.com","reservation-id":\
"r-fea54097"}')

    cmd = pre + 'latest/meta-data/ami-id/'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '"ami-12345678"')

    cmd = pre + 'latest/meta-data/dummy_res/0'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '"res1"')

    cmd = pre + 'latest/Usage/CPU'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '12.12')

    cmd = pre + 'latest/Limits/CPU'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '512')


def test_mmds(test_microvm_with_ssh, network_config):
    """Test the API and guest facing features of the Micro MetaData Service."""
    test_microvm = test_microvm_with_ssh
    test_microvm.spawn()

    # The MMDS is empty at this point.
    response = test_microvm.mmds.get()
    assert test_microvm.api_session.is_status_ok(response.status_code)
    assert response.json() == {}

    # PUT only allows full updates.
    # The json used in MMDS is based on the one from the Instance Meta-data
    # online documentation.
    # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/
    #                                                ec2-instance-metadata.html
    data_store = {
        'latest': {
            'meta-data': {
                'ami-id': 'ami-12345678',
                'reservation-id': 'r-fea54097',
                'local-hostname': 'ip-10-251-50-12.ec2.internal',
                'public-hostname': 'ec2-203-0-113-25.compute-1.amazonaws.com',
                'network': {
                    'interfaces': {
                        'macs': {
                            '02:29:96:8f:6a:2d': {
                                'device-number': '13345342',
                                'local-hostname': 'localhost',
                                'subnet-id': 'subnet-be9b61d'
                            }
                        }
                    }
                }
            }
        }
    }
    response = test_microvm.mmds.put(json=data_store)
    assert test_microvm.api_session.is_status_no_content(response.status_code)

    response = test_microvm.mmds.get()
    assert test_microvm.api_session.is_status_ok(response.status_code)
    assert response.json() == data_store

    # Change only the subnet id using PATCH method.
    patch_json = {
        'latest': {
            'meta-data': {
                'network': {
                    'interfaces': {
                        'macs': {
                            '02:29:96:8f:6a:2d': {
                                'subnet-id': 'subnet-12345'
                            }
                        }
                    }
                }
            }
        }
    }

    response = test_microvm.mmds.patch(json=patch_json)
    assert test_microvm.api_session.is_status_no_content(response.status_code)

    net_ifaces = data_store['latest']['meta-data']['network']['interfaces']
    net_ifaces['macs']['02:29:96:8f:6a:2d']['subnet-id'] = 'subnet-12345'
    response = test_microvm.mmds.get()
    assert test_microvm.api_session.is_status_ok(response.status_code)
    assert response.json() == data_store

    # Now we start the guest and attempt to read some MMDS contents.

    # Set up the microVM with 1 vCPUs, 256 MiB of RAM, no network ifaces, and
    # a root file system with the rw permission. The network interface is
    # added after we get a unique MAC and IP.
    test_microvm.basic_config(vcpu_count=1)
    _tap = test_microvm.ssh_network_config(
        network_config,
        '1',
        allow_mmds_requests=True
    )

    test_microvm.start()

    ssh_connection = net_tools.SSHConnection(test_microvm.ssh_config)

    # Adding a route like this also tests the ARP implementation within the
    # MMDS. We hard code the interface name to `eth0`. The naming is unlikely
    # to change, especially while we keep using VIRTIO net. At some point we
    # could add some functionality to retrieve the interface name based on the
    # MAC address (which we already know) or smt.
    cmd = 'ip route add 169.254.169.254 dev eth0'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '')

    pre = 'curl -s http://169.254.169.254/'

    cmd = pre + 'latest/meta-data/ami-id'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '"ami-12345678"')

    # The request is still valid if we append a trailing slash to a leaf node.
    cmd = pre + 'latest/meta-data/ami-id/'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '"ami-12345678"')

    cmd = pre + 'latest/meta-data/network/interfaces/macs/'\
        '02:29:96:8f:6a:2d/subnet-id'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(stdout, stderr, '"subnet-12345"')

    # Test reading a non-leaf node WITHOUT a trailing slash.
    cmd = pre + 'latest/meta-data'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(
        stdout,
        stderr,
        '{"ami-id":"ami-12345678","local-hostname":\
"ip-10-251-50-12.ec2.internal","network":{"interfaces":\
{"macs":{"02:29:96:8f:6a:2d":{"device-number":"13345342",\
"local-hostname":"localhost","subnet-id":"subnet-12345"}}}},\
"public-hostname":"ec2-203-0-113-25.compute-1.amazonaws.com",\
"reservation-id":"r-fea54097"}')

    # Test reading a non-leaf node with a trailing slash.
    cmd = pre + 'latest/meta-data/'
    _, stdout, stderr = ssh_connection.execute_command(cmd)
    _assert_out(
        stdout,
        stderr,
        '{"ami-id":"ami-12345678","local-hostname":\
"ip-10-251-50-12.ec2.internal","network":{"interfaces":\
{"macs":{"02:29:96:8f:6a:2d":{"device-number":"13345342",\
"local-hostname":"localhost","subnet-id":"subnet-12345"}}}},\
"public-hostname":"ec2-203-0-113-25.compute-1.amazonaws.com",\
"reservation-id":"r-fea54097"}')


def test_mmds_dummy(test_microvm_with_ssh):
    """Test the API and guest facing features of the Micro MetaData Service."""
    test_microvm = test_microvm_with_ssh
    test_microvm.spawn()

    # The MMDS is empty at this point.
    response = test_microvm.mmds.get()
    assert test_microvm.api_session.is_status_ok(response.status_code)
    assert response.json() == {}

    # Test that patch return NotFound when the MMDS is not initialized.
    dummy_json = {
        'latest': {
            'meta-data': {
                'ami-id': 'dummy'
            }
        }
    }
    response = test_microvm.mmds.patch(json=dummy_json)
    assert test_microvm.api_session.is_status_not_found(response.status_code)
    fault_json = {
        "fault_message": "The MMDS resource does not exist."
    }
    assert response.json() == fault_json

    # Test that using the same json with a PUT request, the MMDS data-store is
    # created.
    response = test_microvm.mmds.put(json=dummy_json)
    assert test_microvm.api_session.is_status_no_content(response.status_code)

    response = test_microvm.mmds.get()
    assert test_microvm.api_session.is_status_ok(response.status_code)
    assert response.json() == dummy_json
