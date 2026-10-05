from salt_fixtures import configure_loader_modules  # noqa: F401
import pytest
from render_checks import test_all_sls_render_and_requisites_resolve as check


@pytest.mark.parametrize("family", ["Debian", "RedHat"])
@pytest.mark.parametrize("initialized", [False, True])
@pytest.mark.parametrize("security", [False, True])
def test_sls(rp, family, initialized, security):
    check(rp, family, initialized, security)


@pytest.mark.parametrize('lane', ['storage', 'airgap', 'nightly', 'fips', 'proxy'])
def test_optional_lanes(rp, lane):
    from render_checks import check_optional_lanes
    check_optional_lanes(rp, lane)
