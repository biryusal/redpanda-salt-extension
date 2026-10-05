"""Behavior checks for storage."""

from salt_fixtures import configure_loader_modules  # noqa: F401
import json
from pathlib import Path
from unittest.mock import Mock
import unittest


def test_storage_uuid_must_match_explicit_device(rp):
    rp.__pillar__['redpanda']['storage'] = {
        'devices': ['/dev/disk/by-id/data'],
        'uuid': '6cde2bd7-8592-413b-940d-289c95784e62',
    }
    rp._run = Mock(return_value='wrong-uuid')
    with unittest.TestCase().assertRaisesRegex(ValueError, 'differs'):
        rp.validate_storage()


def test_data_directory_override_cannot_diverge_from_managed_storage(rp):
    rp.__pillar__['redpanda']['node'] = {'redpanda': {'data_directory': '/different'}}
    with unittest.TestCase().assertRaisesRegex(ValueError, 'managed broker'):
        rp.configuration()


def test_storage_mount_must_contain_data_directory(rp):
    rp.__pillar__['redpanda']['storage'] = {
        'devices': ['/dev/disk/by-id/data'],
        'uuid': '6cde2bd7-8592-413b-940d-289c95784e62',
    }
    with unittest.TestCase().assertRaisesRegex(
        ValueError, 'inside the managed storage'
    ):
        rp.validate()


def test_storage_mount_cannot_replace_root(rp):
    rp.__pillar__['redpanda']['storage'] = {
        'devices': ['/dev/disk/by-id/data'],
        'mountpoint': '/',
        'uuid': '6cde2bd7-8592-413b-940d-289c95784e62',
    }
    with unittest.TestCase().assertRaisesRegex(ValueError, 'non-root'):
        rp.validate()


def test_storage_rejects_root_partition_and_parent_disk(rp):
    rp._run = lambda _: json.dumps(
        {
            'blockdevices': [
                {
                    'name': '/dev/nvme0n1',
                    'children': [{'name': '/dev/nvme0n1p1', 'mountpoint': '/'}],
                }
            ]
        }
    )
    for device in ('/dev/nvme0n1', '/dev/nvme0n1p1'):
        rp.__pillar__['redpanda']['storage'] = {'devices': [device]}
        with unittest.TestCase().assertRaisesRegex(ValueError, 'root filesystem'):
            rp.validate_storage_devices()


def test_storage_rejects_foreign_mounted_volume(rp):
    rp.__pillar__['redpanda']['storage'] = {'devices': ['/dev/nvme1n1']}
    rp._run = lambda _: json.dumps(
        {
            'blockdevices': [
                {
                    'name': '/dev/nvme1n1',
                    'children': [{'name': '/dev/nvme1n1p1', 'mountpoint': '/home'}],
                }
            ]
        }
    )
    with unittest.TestCase().assertRaisesRegex(ValueError, 'foreign filesystem'):
        rp.validate_storage_devices()


def test_storage_accepts_existing_data_mount(rp):
    rp.__pillar__['redpanda']['storage'] = {'devices': ['/dev/nvme1n1']}
    rp._run = lambda _: json.dumps(
        {'blockdevices': [{'name': '/dev/nvme1n1', 'mountpoint': '/mnt/vectorized'}]}
    )
    assert rp.validate_storage_devices()


def test_raid_rejects_existing_component_filesystem(rp):
    rp.__pillar__['redpanda']['storage'] = {'devices': ['/dev/nvme1n1', '/dev/nvme2n1']}
    rp._run = lambda _: json.dumps(
        {
            'blockdevices': [
                {'name': '/dev/nvme1n1', 'fstype': 'xfs'},
                {'name': '/dev/nvme2n1'},
            ]
        }
    )
    with unittest.TestCase().assertRaisesRegex(ValueError, 'existing filesystem'):
        rp.validate_storage_devices()


def test_raid_rejects_unmounted_partition_filesystem(rp):
    rp.__pillar__['redpanda']['storage'] = {'devices': ['/dev/nvme1n1', '/dev/nvme2n1']}
    rp._run = lambda _: json.dumps(
        {
            'blockdevices': [
                {
                    'name': '/dev/nvme1n1',
                    'children': [
                        {'name': '/dev/nvme1n1p1', 'fstype': 'ext4', 'mountpoint': None}
                    ],
                },
                {'name': '/dev/nvme2n1'},
            ]
        }
    )
    with unittest.TestCase().assertRaisesRegex(ValueError, 'existing filesystem'):
        rp.validate_storage_devices()


def test_storage_rejects_parent_and_partition_selection(rp):
    rp.__pillar__['redpanda']['storage'] = {
        'devices': ['/dev/nvme1n1', '/dev/nvme1n1p1']
    }
    rp._run = lambda _: json.dumps(
        {
            'blockdevices': [
                {'name': '/dev/nvme1n1', 'children': [{'name': '/dev/nvme1n1p1'}]}
            ]
        }
    )
    with unittest.TestCase().assertRaisesRegex(ValueError, 'overlap'):
        rp.validate_storage_devices()


def test_raid_accepts_components_of_existing_managed_array(rp):
    rp.__pillar__['redpanda']['storage'] = {'devices': ['/dev/nvme1n1', '/dev/nvme2n1']}
    array = {
        'name': '/dev/md0',
        'type': 'raid0',
        'fstype': 'xfs',
        'mountpoint': '/mnt/vectorized',
    }
    rp._run = lambda _: json.dumps(
        {
            'blockdevices': [
                {'name': device, 'fstype': 'linux_raid_member', 'children': [array]}
                for device in rp.__pillar__['redpanda']['storage']['devices']
            ]
        }
    )
    assert rp.validate_storage_devices()


def test_existing_symlink_data_directory_inside_mount_is_supported(rp, tmp_path):
    mount = tmp_path / 'mount'
    (mount / 'data').mkdir(parents=True)
    link = tmp_path / 'data-link'
    link.symlink_to(mount / 'data', target_is_directory=True)
    rp.__pillar__['redpanda'].update(
        data_directory=str(link),
        storage={'devices': ['/dev/disk/by-id/data'], 'mountpoint': str(mount),
                 'uuid': '6cde2bd7-8592-413b-940d-289c95784e62', 'format': False},
    )
    assert rp.validate()


def test_symlink_data_directory_outside_mount_is_rejected(rp, tmp_path):
    mount = tmp_path / 'mount'
    mount.mkdir()
    outside = tmp_path / 'outside'
    outside.mkdir()
    (mount / 'data').symlink_to(outside, target_is_directory=True)
    rp.__pillar__['redpanda'].update(
        data_directory=str(mount / 'data'),
        storage={'devices': ['/dev/disk/by-id/data'], 'mountpoint': str(mount),
                 'uuid': '6cde2bd7-8592-413b-940d-289c95784e62', 'format': False},
    )
    with unittest.TestCase().assertRaisesRegex(ValueError, 'inside the managed'):
        rp.validate()
