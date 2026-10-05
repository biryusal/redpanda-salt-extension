from salt_fixtures import configure_loader_modules  # noqa: F401
import pytest
from graph_checks import check_graph


@pytest.mark.parametrize('initialized', [False, True])
@pytest.mark.parametrize('security', [False, True])
def test_native_salt_node_graph(rp, initialized, security):
    check_graph(rp, initialized, security)


def test_native_salt_rollout_graph(rp):
    check_graph(rp, orchestration=True)
