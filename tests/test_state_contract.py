"""Salt state-return and dry-run contracts using the factories loader fixture."""
from unittest.mock import Mock
import pytest
from support import load


@pytest.fixture
def broker_state():
    return load('salt/_states/redpanda_broker.py')


@pytest.fixture
def configure_loader_modules(broker_state):
    return {broker_state: {'__salt__': {}, '__opts__': {'test': False}}}


def test_prepared_dry_run_forwards_test_and_reports_unknown_result(broker_state):
    begin = Mock(return_value={'needed': True})
    broker_state.__opts__['test'] = True
    broker_state.__salt__['redpanda.transaction_prepare'] = begin
    ret = broker_state.transaction('broker')
    begin.assert_called_once_with(test=True)
    assert ret['result'] is None
    assert ret['changes'] == {}


def test_lifecycle_exception_becomes_failed_salt_state(broker_state):
    broker_state.__salt__['redpanda.broker_start'] = Mock(side_effect=RuntimeError('Drain failed'))
    ret = broker_state.started('broker')
    assert ret['result'] is False
    assert ret['changes'] == {}
    assert ret['comment'] == 'Drain failed'


def test_existing_cluster_config_is_a_successful_noop(broker_state):
    broker_state.__salt__['redpanda.cluster_config'] = Mock(return_value={'changed': False})
    ret = broker_state.cluster_configured('broker')
    assert ret['result'] is True
    assert ret['changes'] == {}
