"""Validate dedicated data devices before provisioning."""

import json
import os


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
