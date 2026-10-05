"""Admin API transport and live connection settings."""

from pathlib import Path
from . import journal
import base64
import json
import ssl
import urllib.error
import urllib.parse
import urllib.request


class _NoRedirect(urllib.request.HTTPRedirectHandler):

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def open_request(req, context):
    opener = urllib.request.build_opener(
        _NoRedirect(), urllib.request.HTTPSHandler(context=context)
    )
    return opener.open(req, timeout=10)


def request(
    c, minion_id, ports, opener, path, method='GET', data=None, local=False, raw=False
):
    from .security import validate_transport

    validate_transport(c)
    nodes = [c['nodes'][minion_id]] if local else list(c['nodes'].values())
    endpoints = [(n, port) for n in nodes for port in ports]
    context = None
    if c['enable_tls']:
        context = ssl.create_default_context(
            cafile=c['paths']['cert_directory'] + '/truststore.pem'
        )
        if c['tls'].get('require_client_auth'):
            context.load_cert_chain(
                c['paths']['cert_directory'] + '/node.crt',
                c['paths']['cert_directory'] + '/node.key',
            )
    for n, port in endpoints:
        host = n['private_ip']
        host = '[' + host + ']' if ':' in host else host
        req = urllib.request.Request(
            f"{('https' if c['enable_tls'] else 'http')}://{host}:{port}/v1/{path}",
            method=method,
            data=(
                (data.encode() if raw else json.dumps(data).encode())
                if data is not None
                else None
            ),
        )
        req.add_header('Content-Type', 'application/json')
        if c['kafka_enable_authorization']:
            token = base64.b64encode(
                (
                    c['sasl'].get('username', 'admin') + ':' + c['sasl']['password']
                ).encode()
            ).decode()
            req.add_header('Authorization', 'Basic ' + token)
        try:
            for redirect in range(5):
                try:
                    with opener(req, context) as response:
                        body = response.read()
                        return json.loads(body) if body else None
                except urllib.error.HTTPError as exc:
                    if exc.code not in (307, 308):
                        raise
                    url = exc.headers.get('Location', '')
                    target = urllib.parse.urlparse(url)
                    allowed = (
                        {n['private_ip'] for n in nodes}
                        if local
                        else {n['private_ip'] for n in c['nodes'].values()}
                    )
                    if (
                        target.hostname not in allowed
                        or target.port not in ports
                        or target.scheme != ('https' if c['enable_tls'] else 'http')
                        or (not target.path.startswith('/v1/'))
                    ):
                        raise RuntimeError(
                            'Admin API redirect outside configured cluster refused'
                        ) from None
                    req = urllib.request.Request(
                        url,
                        method=method,
                        data=req.data,
                        headers=dict(req.header_items()),
                    )
            raise RuntimeError('Too many Admin API redirects')
        except urllib.error.HTTPError as exc:
            if exc.code not in (503, 504):
                raise RuntimeError(
                    f'Admin API {method} {path}: HTTP {exc.code}'
                ) from None
        except (urllib.error.URLError, OSError):
            pass
    raise RuntimeError('No reachable Redpanda Admin API endpoint')


def live_config(ctx):
    if not Path(ctx.path('config')).exists():
        return {}
    text = Path(ctx.path('config')).read_text()
    try:
        config = json.loads(text)
    except json.JSONDecodeError:
        import yaml

        config = yaml.safe_load(text)
    if not isinstance(config, dict):
        raise ValueError('Existing broker config is not a mapping')
    return config


def live_addresses(ctx):
    return live_config(ctx).get('rpk', {}).get('admin_api', {}).get('addresses', [])


def ports(ctx):
    ports = {ctx.config['admin_port']}
    addresses = live_addresses(ctx)
    if journal.pending_path(ctx).exists():
        addresses += json.loads(journal.pending_path(ctx).read_text()).get(
            'admin_addresses', []
        )
    for address in addresses:
        parsed = urllib.parse.urlparse('http://' + address)
        if (
            parsed.hostname in {n['private_ip'] for n in ctx.config['nodes'].values()}
            and parsed.port
        ):
            ports.add(parsed.port)
    return sorted(ports)
