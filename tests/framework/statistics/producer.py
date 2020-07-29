# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Producer of statistics."""

from typing import Callable, Any
from framework import utils


# pylint: disable=R0903
class Producer:
    """Base class for raw results producer."""

    def produce(self) -> Any:
        """Produce raw results."""


class SSHCommand(Producer):
    """Producer from executing ssh commands."""

    def __init__(self, cmd, ssh_connection):
        """Initialize the raw data producer."""
        self._cmd = cmd
        self._ssh_connection = ssh_connection

    def produce(self) -> str:
        """Return the output of the executed ssh command."""
        rc, stdout, stderr = \
            self._ssh_connection.execute_command(self._cmd)
        stderr = stderr.read()
        assert rc == 0
        assert stderr == ""

        return stdout.read()

    @property
    def ssh_connection(self):
        """Return the ssh connection used by the producer.

        The ssh connection used by the producer to execute commands on
        the guest.
        """
        return self._ssh_connection

    @ssh_connection.setter
    def ssh_connection(self, ssh_connection):
        """Set the ssh connection used by the producer."""
        self._ssh_connection = ssh_connection

    @property
    def cmd(self):
        """Return the command executed on guest."""
        return self._cmd

    @cmd.setter
    def cmd(self, cmd):
        """Set the command executed on guest."""
        self._cmd = cmd


class HostCommand(Producer):
    """Producer from executing commands on host."""

    def __init__(self, cmd):
        """Initialize the raw data producer."""
        self._cmd = cmd

    def produce(self):
        """Return output of the executed command."""
        result = utils.run_cmd(self._cmd)

        assert result.returncode == 0
        assert result.stderr == ""

        return result.stdout

    @property
    def cmd(self):
        """Return the command executed on host."""
        return self._cmd

    @cmd.setter
    def cmd(self, cmd):
        """Set the command executed on host."""
        self._cmd = cmd


class LambdaProducer(Producer):
    """Producer from calling python functions."""

    def __init__(self, func, *args):
        """Initialize the raw data producer."""
        self._func = func
        self._args = args

    def produce(self):
        """Call `self._func`."""
        assert callable(self._func)
        return self._func(self._args)

    @property
    def func(self):
        """Return producer function."""
        return self._func

    @func.setter
    def func(self, func: Callable):
        self._func = func

    @property
    def args(self):
        """Return producer function arguments."""
        return self._args

    @args.setter
    def args(self, *args):
        self._args = args
