"""Synthetic Feishu and PD endpoints for a Docker --internal release fixture.

No live Kubernetes endpoint, actual credentials or external connections. The
read-only Kubernetes and platform APIs here are synthetic; Notification Managers
under test are real executables using their ordinary configuration controller.
"""

import json
import socket
import ssl
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

ROOT = Path('/fixture')
LOCK = threading.Lock()
RECORDS = []
MODES = {}
RESOURCES = json.loads((ROOT / 'resources.json').read_text())


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *_args):
        pass

    def reply(self, code, value):
        raw = json.dumps(value).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        try:
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def source(self):
        for name in ('generation-a', 'generation-b', 'legacy-c62'):
            try:
                if socket.gethostbyname(name) == self.client_address[0]:
                    return name
            except OSError:
                continue
        return 'fixture'

    def do_GET(self):
        if self.path == '/control/records':
            with LOCK:
                rows = list(RECORDS)
            return self.reply(200, rows)
        if self.path == '/health':
            return self.reply(200, {'fixture': True})
        path = urlsplit(self.path).path
        query = parse_qs(urlsplit(self.path).query)
        group = {'name': 'notification.kubesphere.io',
                 'versions': [{'groupVersion': 'notification.kubesphere.io/v2beta2', 'version': 'v2beta2'}],
                 'preferredVersion': {'groupVersion': 'notification.kubesphere.io/v2beta2', 'version': 'v2beta2'}}
        if path == '/version':
            return self.reply(200, {'major': '1', 'minor': '28', 'gitVersion': 'v1.28.0'})
        if path == '/api':
            return self.reply(200, {'kind': 'APIVersions', 'versions': ['v1']})
        if path == '/apis':
            return self.reply(200, {'kind': 'APIGroupList', 'groups': [group]})
        if path == '/apis/notification.kubesphere.io':
            return self.reply(200, group)
        known = {'/api/v1': [('configmaps', 'ConfigMap', True), ('secrets', 'Secret', True),
                             ('namespaces', 'Namespace', False)],
                 '/apis/notification.kubesphere.io/v2beta2': [
                     ('notificationmanagers', 'NotificationManager', False),
                     ('receivers', 'Receiver', False), ('configs', 'Config', False),
                     ('routers', 'Router', False), ('silences', 'Silence', False)],
                 '/apis/rbac.authorization.k8s.io/v1': [
                     ('roles', 'Role', True), ('rolebindings', 'RoleBinding', True),
                     ('clusterroles', 'ClusterRole', False), ('clusterrolebindings', 'ClusterRoleBinding', False)]}
        if path in known:
            version = path.removeprefix('/apis/').removeprefix('/api/')
            return self.reply(200, {'kind': 'APIResourceList', 'groupVersion': version,
                                   'resources': [{'name': name, 'singularName': name[:-1], 'kind': kind,
                                                  'namespaced': ns, 'verbs': ['get', 'list', 'watch']}
                                                 for name, kind, ns in known[path]]})
        for base, definitions in known.items():
            if not path.startswith(base + '/'):
                continue
            suffix = path[len(base)+1:].split('/')
            if suffix[0] == 'namespaces' and len(suffix) > 2:
                suffix = suffix[2:]
            definition = next((x for x in definitions if x[0] == suffix[0]), None)
            if definition is None:
                continue
            items = RESOURCES.get(suffix[0], [])
            if query.get('watch') in (['true'], ['1']):
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                try:
                    for item in items:
                        self.wfile.write(json.dumps({'type': 'ADDED', 'object': item}).encode()+b'\n')
                    self.wfile.flush()
                    time.sleep(120)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                self.close_connection = True
                return
            if len(suffix) > 1:
                obj = next((x for x in items if x['metadata']['name'] == suffix[1]), None)
                return self.reply(200, obj) if obj else self.reply(404, {'kind': 'Status', 'code': 404})
            return self.reply(200, {'apiVersion': base.removeprefix('/apis/').removeprefix('/api/'),
                                   'kind': definition[1]+'List', 'metadata': {'resourceVersion': '1'}, 'items': items})
        return self.reply(404, {})

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get('Content-Length', '0')))
        body = json.loads(raw) if raw else {}
        if self.path == '/control/mode':
            with LOCK:
                MODES.update(body)
            return self.reply(200, {'fixture': True})
        if self.path == '/ingress':
            # Local Service stand-in only. This does not claim Kubernetes
            # endpoint propagation or live-cluster source-policy coverage.
            target = MODES.get('target', 'generation-a')
            req = urllib.request.Request(
                'http://' + target + ':19094/api/v2/test/alerts', raw,
                {'Content-Type': 'application/json',
                 'Authorization': 'Bearer ' + 't' * 32})
            try:
                with urllib.request.urlopen(req, timeout=5) as response:
                    code, result = response.status, response.read()
            except urllib.error.HTTPError as error:
                code, result = error.code, error.read()
            except OSError:
                return self.reply(503, {'fixture': 'target unavailable'})
            try:
                result = json.loads(result)
            except ValueError:
                result = {'response': result.decode(errors='replace')}
            return self.reply(code, result)
        if '/auth/' in self.path and 'token' in self.path:
            return self.reply(200, {'code': 0, 'tenant_access_token': 'synthetic-local-only',
                                    'app_access_token': 'synthetic-local-only', 'expire': 7200})
        source = self.source()
        mode = MODES.get(source, 'healthy')
        if self.path == '/sync/slow':
            time.sleep(.5)
        if self.path == '/sync/failure':
            mode = 'reject'
        if mode == 'slow':
            time.sleep(2)
        with LOCK:
            index = len(RECORDS) + 1
            row = {'source': source, 'path': self.path, 'body': body,
                   'at': time.monotonic(), 'mode': mode, 'sequence': index}
            RECORDS.append(row)
            with (ROOT / 'platform-records.jsonl').open('a') as output:
                output.write(json.dumps(row) + '\n')
        if mode == 'response_lost':
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        if mode == 'reject':
            return self.reply(429, {'code': 99991400, 'msg': 'synthetic rate limit'})
        if self.path.startswith('/open-apis/'):
            return self.reply(200, {'code': 0, 'data': {'message_id': 'om_fixture_' + str(index)},
                                    'message_id': 'om_fixture_' + str(index)})
        return self.reply(200, {'fixture': True})


if __name__ == '__main__':
    http = ThreadingHTTPServer(('0.0.0.0', 8080), Handler)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    https = ThreadingHTTPServer(('0.0.0.0', 443), Handler)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(ROOT / 'cert.pem', ROOT / 'key.pem')
    https.socket = tls.wrap_socket(https.socket, server_side=True)
    https.serve_forever()
