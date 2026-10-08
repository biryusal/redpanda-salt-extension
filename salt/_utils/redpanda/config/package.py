"""Select broker packages and verify their installed versions."""

from .. import lifecycle


def packages(ctx):
    c = ctx.config
    split = c['version'] == 'latest' or tuple(
        (int(x) for x in c['version'].split('-')[0].split('.')[:2])
    ) >= (24, 2)
    names = ['redpanda', 'redpanda-rpk', 'redpanda-tuner'] if split else ['redpanda']
    if c['enable_fips']:
        names += ['redpanda-fips', 'redpanda-rpk-fips']
    return names


def validate_packages(ctx):
    c = ctx.config
    if c['version'] != 'latest' and lifecycle.package_changes(ctx):
        raise ValueError('Installed broker packages do not match the requested version')
    return True
