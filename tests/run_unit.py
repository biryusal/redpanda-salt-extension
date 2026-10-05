"""Convenience entry point; pytest is the single test runner."""
from pathlib import Path
import pytest

if __name__ == '__main__':
    root = Path(__file__).resolve().parent
    files = ['configuration', 'admin', 'cluster', 'storage', 'security',
             'adapter', 'lifecycle_phases', 'rollout', 'review_regressions', 'state_contract']
    raise SystemExit(pytest.main(['-q'] + [str(root / ('test_' + name + '.py')) for name in files]))
