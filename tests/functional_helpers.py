"""Isolated Salt loaders using the official factories helper."""
from unittest.mock import patch
import yaml
import salt.loader
import salt.config
from saltfactories.utils.functional import Loaders
from support import ROOT


def make_loaders(tmp_path, pillar, family='Debian', test=False):
    pillar = dict({'allow_insecure_sasl': True}, **pillar)
    pillar_root = tmp_path / 'pillar'
    pillar_root.mkdir()
    (pillar_root / 'top.sls').write_text(yaml.safe_dump({'base': {'*': ['redpanda']}}))
    (pillar_root / 'redpanda.sls').write_text(yaml.safe_dump({'redpanda': pillar}))
    opts = salt.config.minion_config(None)
    opts.update(id='broker-1', file_client='local', local=True, cachedir=str(tmp_path / 'cache'), pki_dir=str(tmp_path / 'pki'), sock_dir=str(tmp_path / 'sock'), extension_modules=str(tmp_path / 'ext'), file_roots={'base': [str(ROOT / 'salt')]}, pillar_roots={'base': [str(pillar_root)]}, utils_dirs=[str(ROOT / 'salt/_utils')], module_dirs=[str(ROOT / 'salt/_modules')], states_dirs=[str(ROOT / 'salt/_states')], grains={'id': 'broker-1', 'os_family': family, 'os': 'Debian' if family == 'Debian' else 'Rocky', 'osrelease': '12' if family == 'Debian' else '9', 'osmajorrelease': 12 if family == 'Debian' else 9, 'cpuarch': 'x86_64', 'kernel': 'Linux'}, pillar={'redpanda': pillar}, test=test)
    config_file = tmp_path / 'minion'
    config_file.write_text(yaml.safe_dump({'grains': opts['grains']}))
    opts['conf_file'] = str(config_file)
    # These are compiler/dispatcher tests for simulated Linux platforms.
    # Avoid collecting macOS host hardware/DNS grains on every loader reset.
    with patch('salt.loader.grains', return_value=opts['grains']):
        loaders = Loaders(opts)
    return loaders
