"""Validate dedicated data devices before provisioning."""

import json
import os
import re
import uuid
import yaml


def validate_devices(c, run):
    """Reject root-device ancestry and mounted foreign volumes before RAID/mkfs."""
    devices = [os.path.realpath(d) for d in c['storage']['devices']]
    if len(devices) != len(set(devices)):
        raise ValueError('Storage devices resolve to duplicates')
    tree = json.loads(
        run(['lsblk', '--json', '--paths', '--output', 'NAME,TYPE,MOUNTPOINT,FSTYPE'])
    )
    paths = {}
    root_devices = set()
    overlaps = set()

    def visit(node, ancestors=()):
        name = os.path.realpath(node['name'])
        lineage = ancestors + (name,)
        if name in devices and any((parent in devices for parent in ancestors)):
            overlaps.add(name)
        paths.setdefault(name, []).append(node)
        if node.get('mountpoint') == '/':
            root_devices.update(lineage)
        for child in node.get('children', []):
            visit(child, lineage)

    for node in tree['blockdevices']:
        visit(node)
    if overlaps:
        raise ValueError('Storage devices overlap in the block-device topology')

    def component_filesystems(node, descendant=False):
        if descendant and node.get('type', '').startswith('raid'):
            return False
        return node.get('fstype') not in (None, '', 'linux_raid_member') or any(
            (
                component_filesystems(child, descendant=True)
                for child in node.get('children', [])
            )
        )

    def mounts(node):
        return ([node['mountpoint']] if node.get('mountpoint') else []) + [
            m for child in node.get('children', []) for m in mounts(child)
        ]

    allowed = c['storage'].get('mountpoint', '/mnt/vectorized')
    for device in devices:
        if device not in paths or device in root_devices:
            raise ValueError('Storage device is missing or backs the root filesystem')
        if len(devices) > 1 and any(
            (component_filesystems(node) for node in paths[device])
        ):
            raise ValueError('RAID component contains an existing filesystem')
        if any((m != allowed for node in paths[device] for m in mounts(node))):
            raise ValueError('Storage device contains a mounted foreign filesystem')
    return True


def validate_storage(ctx):
    c = ctx.config
    devices = c['storage']['devices']
    device = devices[0] if len(devices) == 1 else '/dev/md0'
    actual = ctx.run(['blkid', '-s', 'UUID', '-o', 'value', device]).strip()
    if actual.lower() != c['storage']['uuid'].lower():
        raise ValueError(
            'Data-device filesystem UUID differs from configured storage UUID'
        )
    return True


def validate_storage_devices(ctx):
    return validate_devices(ctx.config, ctx.run)


def discover(config, run_all):
    devices = config['storage']['devices']
    if len(devices) != 1:
        raise ValueError('Automatic XFS resolution requires one explicit device')
    device = devices[0]
    if not re.fullmatch(r'/dev/[A-Za-z0-9_./-]+', device) or not os.path.exists(device):
        raise ValueError('Explicit data device is invalid or missing')
    def inspect(args, optional=False):
        result = run_all(args, python_shell=False)
        if result['retcode'] and not (optional and result['retcode'] == 1):
            raise ValueError('Cannot inspect attached storage')
        return result['stdout'].strip()
    signature = run_all(['blkid', '-p', '-o', 'export', device], python_shell=False)
    blank = (signature['retcode'] == 2 and not signature['stdout'].strip()
             and not signature.get('stderr', '').strip())
    if signature['retcode'] and not blank:
        raise ValueError('Cannot inspect data-device signatures')
    metadata = dict(line.split('=', 1) for line in signature['stdout'].splitlines() if '=' in line)
    if not blank and (metadata.get('TYPE') != 'xfs' or metadata.get('PTTYPE')):
        raise ValueError('Data volume must be XFS or an unused whole device')
    filesystem_uuid = config['storage'].get('uuid') or str(uuid.uuid5(uuid.NAMESPACE_URL, 'redpanda-device:' + device)) if blank else metadata.get('UUID')
    if not filesystem_uuid or not re.fullmatch(r'[0-9a-fA-F-]{36}', filesystem_uuid):
        raise ValueError('Invalid XFS filesystem UUID')
    expected_uuid = config['storage'].get('uuid')
    if expected_uuid and filesystem_uuid.lower() != expected_uuid.lower():
        raise ValueError('Existing XFS UUID differs from configured storage UUID')
    desired = config
    broker_config = config['paths']['config']
    existing = os.path.exists(broker_config)
    if existing:
        with open(broker_config) as stream:
            data = yaml.safe_load(stream)['redpanda']['data_directory']
    else:
        data = desired.get('data_directory', '/var/lib/redpanda/data')
    resolved = os.path.realpath(data)
    mountpoint = desired.get('storage', {}).get('mountpoint')
    if not blank:
        for extra in [[], ['--fstab']]:
            raw = inspect(['findmnt', '-J', '--evaluate', *extra, '-S', 'UUID=' + filesystem_uuid, '-o', 'TARGET'], optional=True)
            if raw:
                candidates = [entry['target'] for entry in json.loads(raw)['filesystems']
                              if os.path.commonpath([resolved, os.path.realpath(entry['target'])]) == os.path.realpath(entry['target'])]
                if candidates:
                    detected = max(candidates, key=len)
                    if mountpoint and os.path.realpath(mountpoint) != os.path.realpath(detected):
                        raise ValueError('Desired mountpoint conflicts with existing data-volume mount')
                    mountpoint = detected
                    break
    if not mountpoint:
        if existing and not blank:
            raise ValueError('Set storage.mountpoint for recovery when neither mount nor fstab describes the existing data volume')
        mountpoint = resolved
    if mountpoint == '/' or os.path.commonpath([resolved, os.path.realpath(mountpoint)]) != os.path.realpath(mountpoint):
        raise ValueError('Data directory must reside on the dedicated data mount')
    if blank and existing:
        raise ValueError('Existing broker configuration points at an uninitialized volume; refusing automatic initialization')
    if blank and not config['storage'].get('initialize', False):
        raise ValueError('Unused volume requires storage.initialize: true')
    return {'data_directory': data, 'storage': {'devices': [device], 'uuid': filesystem_uuid,
            'mountpoint': mountpoint, 'format': blank}}
