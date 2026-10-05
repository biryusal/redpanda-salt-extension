import pytest
import sys
import tempfile
from support import broker_module


@pytest.fixture
def rp(tmp_path):
    import time
    original = time.sleep
    yield broker_module(tmp_path, inject=False)
    time.sleep = original


@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    # The factories default binds 0.0.0.0; macOS cannot reliably connect to it
    # when the server sends its shutdown sentinel. All our clients are local.
    log_server = config.pluginmanager.get_plugin('saltfactories-log-server')
    if log_server is not None:
        log_server.log_host = '127.0.0.1'


@pytest.fixture(scope='session')
def salt_factories_config():
    # Separate concurrent runs and keep Darwin UNIX-socket paths short.
    with tempfile.TemporaryDirectory(prefix='rp-sf-', dir='/tmp' if sys.platform == 'darwin' else None) as root:
        yield {'root_dir': root}


@pytest.hookimpl(hookwrapper=True)
def pytest_sessionfinish(session):
    yield
    # Flush Salt's bootstrap logger while pytest logging streams are still open.
    # Otherwise Salt's atexit handler writes buffered expected errors to a closed stream.
    import salt._logging
    salt._logging.shutdown_temp_handler()
