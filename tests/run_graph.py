"""Broker dependency graphs checked by the native Salt DAG implementation."""
import itertools
from pathlib import Path
import tempfile
import unittest
from graph_checks import check_graph
from support import broker_module


def run(initialized, security, orchestration=False):
    with tempfile.TemporaryDirectory() as directory:
        check_graph(broker_module(Path(directory)), initialized, security, orchestration)


suite = unittest.TestSuite(
    unittest.FunctionTestCase(lambda i=i, s=s: run(i, s), description=f'initialized={i}, security={s}')
    for i, s in itertools.product([False, True], repeat=2)
)
suite.addTest(unittest.FunctionTestCase(lambda: run(False, False, True), description='serial orchestration'))
if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    raise SystemExit(not result.wasSuccessful())
