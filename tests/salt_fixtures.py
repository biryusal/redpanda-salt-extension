"""Fixtures consumed by pytest-salt-factories' loader plugin."""
import pytest
from support import broker_dunders


@pytest.fixture
def configure_loader_modules(rp):
    return {rp: broker_dunders(rp.core)}
