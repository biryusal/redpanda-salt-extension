"""Render checks using Jinja2/PyYAML; no running Salt daemon required."""
import itertools
from pathlib import Path
import tempfile
import unittest
from support import broker_module
from render_checks import test_all_sls_render_and_requisites_resolve, check_optional_lanes


def run(family, initialized, security):
    with tempfile.TemporaryDirectory() as directory:
        test_all_sls_render_and_requisites_resolve(broker_module(Path(directory)), family, initialized, security)


suite = unittest.TestSuite(
    unittest.FunctionTestCase(lambda f=f, i=i, s=s: run(f, i, s), description=f'{f}, initialized={i}, security={s}')
    for f, i, s in itertools.product(['Debian', 'RedHat'], [False, True], [False, True])
)
def run_optional(lane):
    with tempfile.TemporaryDirectory() as directory:
        check_optional_lanes(broker_module(Path(directory)), lane)


suite.addTests(unittest.FunctionTestCase(lambda lane=lane: run_optional(lane), description=lane)
               for lane in ['storage', 'airgap', 'nightly', 'fips', 'proxy'])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    raise SystemExit(not result.wasSuccessful())
