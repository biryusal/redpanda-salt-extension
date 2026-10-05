"""Command boundary: argv arrays, quiet output, credentials in env."""


def run(salt, argv, env=None):
    kwargs = {'python_shell': False, 'output_loglevel': 'quiet'}
    if env is not None:
        kwargs['env'] = env
    result = salt['cmd.run_all'](argv, **kwargs)
    if result['retcode']:
        raise RuntimeError('Command failed: ' + ' '.join(argv[:2]))
    return result['stdout']
