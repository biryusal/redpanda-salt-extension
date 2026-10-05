"""Behavior checks for adapter."""

from salt_fixtures import configure_loader_modules  # noqa: F401
import json
from pathlib import Path
from unittest.mock import Mock
import unittest


def test_state_dry_run_and_sanitized_return(rp):
    from support import load

    state = load('salt/_states/redpanda_broker.py')
    state.__opts__ = {'test': True}
    state.__salt__ = {'redpanda.transaction_prepare': lambda test: {'needed': True}}
    result = state.transaction('broker')
    assert result['result'] is None
    assert result['changes'] == {}
