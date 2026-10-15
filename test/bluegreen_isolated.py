#!/usr/bin/env python3
"""Real NM binaries, separate Bolt volumes and AM peers on Docker --internal.

The Python platform and Service fixtures are local only. No Kubernetes, cloud,
production credentials or actual messaging platform is accessed. Use --help.
Evidence distinguishes implemented protocol cases from c62/bootstrap blockers.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import uuid

OLD = ('harbor.op.zillizcloud.com/devops/notification-manager@'
       'sha256:c62b209602c3b8220a39da7ec07e10e172836025be3e88813776b0b8536c5b28')
RC7 = 'sha256:f208d96a27eda27d2bc23fccb5704606a3b408d91e46e5f7b908e20da03d0441'
PROD = 'oc_6c65c3f33939cb742e54cd633f95d3a2'
TEST = 'oc_fbb2c50fed6ea3bd0f0777bea6f35358'
READ, CONTROL = 'r' * 32, 'c' * 32


def run(*args, check=True):
    return subprocess.run(args, check=check, text=True, capture_output=True)


class Fixture:
    def __init__(self, args):
        self.args = args
        self.root = args.output.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.prefix = 'nm-bg-' + uuid.uuid4().hex[:10]
        self.net = self.prefix + '-internal'
        self.names = []
        self.events = []
        self.begin = time.monotonic()
        self.deadline = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=180)).isoformat()
        self.operation = 'isolated-' + self.prefix
        self.expected = []
        self.accepted_at = {}
        self.send_delay_upper_bounds = {}
        self.samples = []
        self.stop_sampling = threading.Event()

    def event(self, case, **fields):
        self.events.append(dict(case=case, elapsed_seconds=round(time.monotonic()-self.begin, 3), **fields))

    def docker(self, *args, check=True):
        return run('docker', *args, check=check)

    def request(self, host, path, body=None, token=None, method=None):
        # Execute the HTTP client inside the isolated network; no host ports.
        code = '''import json,urllib.request,urllib.error,sys
x=json.loads(sys.argv[1]); d=None if x['body'] is None else json.dumps(x['body']).encode()
r=urllib.request.Request('http://'+x['host']+x['path'],d,method=x['method'])
r.add_header('Content-Type','application/json')
if x['token']: r.add_header('Authorization','Bearer '+x['token'])
try:
 with urllib.request.urlopen(r,timeout=8) as p: c,b=p.status,p.read()
except urllib.error.HTTPError as p: c,b=p.code,p.read()
try: b=json.loads(b)
except ValueError: b=b.decode(errors='replace')
print(json.dumps([c,b]))'''
        request = dict(host=host, path=path, body=body, token=token,
                       method=method or ('GET' if body is None else 'POST'))
        return json.loads(self.docker('exec', self.prefix+'-mock', 'python', '-c', code,
                                      json.dumps(request)).stdout)

    def wait(self, predicate, timeout=15, label='condition'):
        deadline = time.monotonic()+timeout
        last = None
        while time.monotonic() < deadline:
            try:
                last = predicate()
                if last:
                    return last
            except (subprocess.CalledProcessError, ValueError, OSError):
                pass
            time.sleep(.2)
        raise AssertionError(f'timeout waiting for {label}; last={last!r}')

    def start(self, alias, old=False, mode='active', profiles=True):
        state = self.root / alias
        state.mkdir(exist_ok=True)
        state.chmod(0o777)
        name = self.prefix+'-'+alias
        self.names.append(name)
        env = dict(KUBECONFIG='/fixture/kubeconfig',NAMESPACE='kubesphere-monitoring-system',
                   NM_NOTIFICATION_SPOOL_PATH='/state/spool.db',
                   NM_DELIVERY_PROFILES_FILE='/fixture/profiles.json', NM_CONFIGURATION_NAME='notification-manager',
                   NM_TEST_INTAKE_TOKEN='t'*32, NM_FORMAL_INTAKE_TOKEN='f'*32,
                   NM_PROFILE_CONTROL_TOKEN='p'*32, SSL_CERT_FILE='/fixture/cert.pem')
        if not profiles:
            for key in ['NM_DELIVERY_PROFILES_FILE','NM_TEST_INTAKE_TOKEN','NM_FORMAL_INTAKE_TOKEN','NM_PROFILE_CONTROL_TOKEN']:
                env.pop(key)
        if not old:
            env.update(NM_INSTANCE_ID=alias, NM_INSTANCE_INITIAL_MODE=mode,
                       NM_INSTANCE_READ_TOKEN_FILE='/fixture/read.token',
                       NM_INSTANCE_CONTROL_TOKEN_FILE='/fixture/control.token',
                       NM_INSTANCE_PROTOCOL_KEY_FILE='/fixture/proof.key')
        cmd = ['run','-d','--pull','never','--name',name,'--network',self.net,'--network-alias',alias,
               '--platform','linux/amd64','--cpus','0.8','--memory','1g',
               '-v',str(self.root)+':/fixture:ro','-v',str(state)+':/state']
        for key, value in env.items():
            cmd += ['-e',key+'='+value]
        if old=='rc7':
            cmd += ['--entrypoint','/workspace/notification-manager']
        elif not old:
            cmd += ['-v',str(self.args.binary.resolve())+':/new-nm:ro','--entrypoint','/new-nm']
        cmd += [RC7 if old=='rc7' else OLD]
        if old!='rc7':
            cmd += ['--store.type=notification_durable']
        cmd += ['--webhook.address=:19094']
        if not profiles:
            cmd += ['--worker.timeout=200ms','--webhook.timeout=1s']
        self.docker(*cmd)
        if old=='rc7':
            actual=self.docker('exec',name,'sha256sum','/workspace/notification-manager').stdout.split()[0]
            assert actual=='ac28d0c192d2b1ba043b57e0876b3fbc558538bd1c908b2899f23bc7db0b6b62',actual
        self.wait(lambda:self.request(alias+':19094','/-/ready')[0]==200,label=alias+' Ready')

    def prepare(self):
        for name, content in [('read.token',READ),('control.token',CONTROL),('proof.key','k'*32)]:
            (self.root/name).write_text(content)
        run('openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-keyout',str(self.root/'key.pem'),'-out',str(self.root/'cert.pem'),
            '-subj','/CN=open.feishu.cn','-addext',
            'subjectAltName=DNS:open.feishu.cn,DNS:open.larksuite.com')
        template = ('{{define "fixture"}}{"header":{"title":{"tag":"plain_text",'
                    '"content":"{{range .Alerts}}{{.Labels.alertname}} {{.Status}}{{end}}"}},'
                    '"elements":[{"tag":"action","actions":['
                    '{"tag":"button","value":{"action":"acknowledge"}},'
                    '{"tag":"button","value":{"action":"create_group"}},'
                    '{"tag":"button","value":{"action":"silence"}}]}]}{{end}}')
        config = dict(environment='test',cluster='synthetic-only',template=template,
                      configs=[dict(metadata=dict(name='infra'),spec=dict(feishu=dict(
                          appID=dict(value='synthetic-local-app'),appSecret=dict(value='synthetic-only-secret'))))],
                      receivers=[dict(metadata=dict(name='critical'),spec=dict(feishu=dict(
                          alertSelector=dict(matchLabels=dict(severity='critical')),chatIDs=[PROD],
                          template='fixture',tmplType='interactive')))])
        (self.root/'static.json').write_text(json.dumps(config))
        version='notification.kubesphere.io/v2beta2'
        def resource(kind,name,spec,labels=None):
            return dict(apiVersion=version,kind=kind,metadata=dict(name=name,uid='synthetic-'+name,
                resourceVersion='1',labels=labels or {}),spec=spec)
        import base64
        secret=dict(apiVersion='v1',kind='Secret',metadata=dict(name='fixture-secret',namespace='kubesphere-monitoring-system',
                    uid='synthetic-secret',resourceVersion='1'),data={'app-secret':base64.b64encode(b'synthetic-only-secret').decode()})
        fconfig=config['configs'][0]['spec']['feishu']
        fconfig['appSecret']={'valueFrom':{'secretKeyRef':{'name':'fixture-secret','namespace':'kubesphere-monitoring-system','key':'app-secret'}}}
        receiver=config['receivers'][0]['spec']['feishu']
        receiver.update(chatids=receiver.pop('chatIDs'),enabled=True,
                        feishuConfigSelector={'matchLabels':{'type':'default','app':'feishu'}},
                        template='{{ template "fixture" . }}',
                        tmplText={'name':'fixture-template','namespace':'kubesphere-monitoring-system'})
        manager=resource('NotificationManager','notification-manager',dict(
            replicas=1,defaultConfigSelector={'matchLabels':{'type':'default'}},
            receivers=dict(tenantKey='user',globalReceiverSelector={'matchLabels':{'type':'global'}},
                           tenantReceiverSelector={'matchLabels':{'type':'tenant'}},options={'global':{'cluster':'synthetic-only'}}),
            groupLabels=['alertname','cluster'],batchMaxSize=100,batchMaxWait='1s',routePolicy='All',
            template={'text':{'name':'fixture-template','namespace':'kubesphere-monitoring-system'}}))
        resources=dict(notificationmanagers=[manager],
            receivers=[resource('Receiver','critical',{'feishu':receiver},{'type':'global'})],
            configs=[resource('Config','infra',{'feishu':fconfig},{'type':'default','app':'feishu'})],
            configmaps=[dict(apiVersion='v1',kind='ConfigMap',metadata=dict(name='fixture-template',
                namespace='kubesphere-monitoring-system',uid='synthetic-template',resourceVersion='1'),data={'fixture':template})],
            secrets=[secret],namespaces=[])
        (self.root/'resources.json').write_text(json.dumps(resources))
        (self.root/'kubeconfig').write_text(json.dumps(dict(apiVersion='v1',kind='Config',
            clusters=[dict(name='synthetic',cluster=dict(server='http://mock:8080'))],
            contexts=[dict(name='synthetic',context=dict(cluster='synthetic',user='synthetic'))],
            users=[dict(name='synthetic',user={})],**{'current-context':'synthetic'})))
        profiles = dict(version=1,initial_test_version='v2',retry_dedupe_window='5s',repeat_interval='8s',
                        profiles=[dict(id='test',version='v2',card_owner_id='synthetic-test',receiver='critical',
                                       source_chat_id=PROD,test_chat_id=TEST),
                                  dict(id='formal',version='v2',card_owner_id='synthetic-formal')])
        (self.root/'profiles.json').write_text(json.dumps(profiles))
        self.docker('network','create','--internal',self.net)
        name = self.prefix+'-mock'
        self.names.append(name)
        self.docker('run','-d','--pull','never','--name',name,'--network',self.net,'--network-alias','mock',
                    '--network-alias','open.feishu.cn','--network-alias','open.larksuite.com',
                    '-v',str(self.root)+':/fixture','-v',str(Path(__file__).with_name('bluegreen_mock.py').resolve())+
                    ':/mock.py:ro','python:3-alpine','python','/mock.py')
        self.wait(lambda:self.request('mock:8080','/health')[0]==200,label='synthetic platform')
        self.start('generation-a')
        self.start('generation-b',mode='standby')
        self.start('legacy-c62',old=True)
        self.event('isolation',network_internal=True,host_ports=[],production_calls=0,
                   cpu_limit_per_nm=.8,memory_limit_bytes=1024*1024*1024,
                   old_image=OLD,new_binary_sha256=hashlib.sha256(self.args.binary.read_bytes()).hexdigest())
        def sample():
            while not self.stop_sampling.is_set():
                stats=self.docker('stats','--no-stream','--format','{{json .}}',*self.names,check=False)
                self.samples.append(dict(elapsed_seconds=round(time.monotonic()-self.begin,3),
                                         containers=[json.loads(x) for x in stats.stdout.splitlines() if x.startswith('{')],
                                         bolt_bytes={p.parent.name:p.stat().st_size for p in self.root.glob('*/spool.db')}))
                self.stop_sampling.wait(1)
        self.sampler=threading.Thread(target=sample,daemon=True)
        self.sampler.start()

    def state(self, who):
        code, body = self.request(who+':19094','/internal/instance',token=READ)
        assert code == 200, (code,body)
        return body['state']

    def command(self, who, action, peer, proof=None, expected_code=200):
        state = self.state(who)
        command = dict(operation_id=self.operation,expected_revision=state['revision'],action=action,
                       peer_id=peer,deadline=self.deadline,max_send_delay_millis=150000,
                       sync_remaining_millis=1000,actor='isolated-harness',evidence='synthetic platform only')
        if proof is not None:
            command['retirement_proof'] = proof
        code, body = self.request(who+':19094','/internal/instance/control',command,CONTROL)
        assert code == expected_code, (who,action,code,body)
        return body

    def export(self, who, final=False):
        state = self.state(who)
        code, manifest = self.request(who+':19094','/internal/instance/export',dict(
            operation_id=self.operation,expected_revision=state['revision'],final=final),CONTROL)
        assert code == 200,(code,manifest)
        (self.root/(who+('-final' if final else '-preseed')+'-synthetic-manifest.json')).write_text(json.dumps(manifest))
        if final:
            code, again = self.request(who+':19094','/internal/instance/export?operation_id='+self.operation+
                                      '&checksum='+manifest['snapshot_checksum'],token=CONTROL)
            assert code==200 and again==manifest,'lost-response GET changed final export'
        return manifest

    def transfer(self, source, destination, final=False):
        manifest = self.export(source,final)
        state = self.state(destination)
        command = dict(operation_id=self.operation,expected_revision=state['revision'],action='import',
                       peer_id=source,deadline=self.deadline,max_send_delay_millis=150000,
                       sync_remaining_millis=1000,actor='isolated-harness',evidence='synthetic exact export')
        (self.root/(destination+'-import-debug.json')).write_text(json.dumps(dict(state=state,command=command)))
        code, body = self.request(destination+':19094','/internal/instance/import',
                                  dict(command=command,manifest=manifest),CONTROL)
        assert code==200,(source,destination,code,body)
        return manifest['snapshot_checksum']

    def records(self):
        return self.request('mock:8080','/control/records')[1]

    def cards(self):
        return [r for r in self.records() if r['path'].startswith(('/open-apis/im/','/open-apis/message/'))]

    @staticmethod
    def content(row):
        body=row['body']
        return body['card'] if 'card' in body else json.loads(body['content'])

    def send(self, who, name, status='firing', start=0, want=200, remember=True):
        base = dt.datetime(2026,10,15,tzinfo=dt.timezone.utc)+dt.timedelta(seconds=start)
        payload = dict(receiver='fixture',groupKey='group-'+name,status=status,
                       externalURL='http://synthetic-peer',groupLabels=dict(alertname=name),
                       alerts=[dict(status=status,labels=dict(alertname=name,severity='critical',cluster='fixture',
                                                             namespace='fixture'),annotations=dict(summary=name),
                                    startsAt=base.isoformat(),endsAt=('2040-01-01T00:00:00Z' if status=='firing'
                                                                    else (base+dt.timedelta(seconds=1)).isoformat()))])
        code, body = self.request(who+':19094','/api/v2/test/alerts',payload,'t'*32)
        assert code==want,(name,code,body)
        if remember and want==200:
            self.expected.append(name+' '+status)
            self.accepted_at.setdefault(name+' '+status,[]).append(time.monotonic())
        return payload

    def count(self, n):
        self.wait(lambda:len(self.cards())>=n,timeout=25,label=f'{n} platform cards')
        assert len(self.cards())==n,(len(self.cards()),n)
        now=time.monotonic()
        for row in self.cards():
            title=self.content(row)['header']['title']['content']
            if row['sequence'] not in self.send_delay_upper_bounds and self.accepted_at.get(title):
                accepted=self.accepted_at[title].pop(0)
                self.send_delay_upper_bounds[row['sequence']]=round(now-accepted,3)

    def handoff(self, source, destination):
        self.command(source,'close-admission',destination)
        checksum = self.transfer(source,destination,True)
        old = self.command(source,'retire',destination)
        self.command(destination,'activate',source,old['retirement_proof'])
        self.event('handoff',source=source,destination=destination,checksum=checksum,
                   source_phase=self.state(source)['phase'],destination_phase=self.state(destination)['phase'])

    def execute(self):
        self.prepare()
        a,b='generation-a','generation-b'
        assert self.request(b+':19094','/internal/instance')[0]==401
        assert self.request(b+':19094','/internal/instance/control',{},READ)[0]==401
        assert self.request('legacy-c62:19094','/internal/instance',token=READ)[0]==404
        self.send('legacy-c62','legacy-unmodified',remember=False)
        self.count(1)
        self.event('c62_bootstrap',status='BLOCKED',reason='locked c62 has no online transfer/retirement protocol',
                   old_still_ready=self.request('legacy-c62:19094','/-/ready')[0]==200)
        self.send(a,'initial')
        self.count(2)
        assert self.request(b+':19094','/api/v2/test/verify',{},'t'*32)[0] in (403,503)
        self.command(a,'begin-active',b)
        self.command(b,'begin-standby',a)
        self.transfer(a,b)
        self.send(a,'old-tail')
        self.count(3)
        self.send(b,'old-tail',remember=False)  # peer retry across generations
        self.send(b,'lifecycle','firing',start=10)
        self.send(b,'lifecycle','resolved',start=10)
        self.send(b,'lifecycle','firing',start=20)
        assert len(self.cards())==3,'standby sent platform traffic'
        self.handoff(a,b)
        self.count(6)
        self.docker('restart',self.prefix+'-'+a)
        self.wait(lambda:self.state(a)['phase']=='retired',label='persistent retired fence')
        self.send(a,'retired-reject',want=503,remember=False)
        before=len(self.cards())
        # Same-operation reverse handoff; both independent PVCs retained.
        self.command(a,'rollback-standby',b)
        self.transfer(b,a)
        self.send(a,'rollback-buffer')
        assert len(self.cards())==before
        self.handoff(b,a)
        self.count(7)
        self.send(a,'after-rollback')
        self.count(8)
        time.sleep(8.2)
        self.send(a,'after-rollback')  # legitimate repeat beyond the configured AM repeat interval
        self.count(9)
        cards=self.cards()
        titles=[self.content(r)['header']['title']['content'] for r in cards]
        assert titles[1:]==self.expected,(titles,self.expected)
        for row in cards:
            assert row['body'].get('receive_id',row['body'].get('chat_id'))==TEST,row['body']
            content=self.content(row)
            assert [x['value']['action'] for x in content['elements'][0]['actions']]==[
                'acknowledge','create_group','silence']
        self.event('normal_and_reverse',status='PASS',accepted_expected=len(self.expected),
                   platform_cards=len(cards)-1,extra_duplicates=0,missing=0,order_correct=True,
                   distinct_pvcs=True,profile_switches=0,
                   max_observed_send_delay_upper_bound_seconds=max(self.send_delay_upper_bounds.values()))
        self.synchronous_compatibility()
        self.real_am_peers(a)
        # A real send with response lost becomes unknown; it must block drain.
        self.request('mock:8080','/control/mode',{a:'response_lost'})
        self.send(a,'unknown-result')
        self.wait(lambda:self.request(a+':19094','/internal/instance',token=READ)[1]['spool'].get(
            'counts',{}).get('unknown',0)>0,timeout=15,label='persistent unknown')
        count_after_unknown=len(self.cards())
        self.docker('restart',self.prefix+'-'+a)
        self.wait(lambda:self.request(a+':19094','/internal/instance',token=READ)[1]['spool'].get(
            'counts',{}).get('unknown',0)>0,label='unknown survives restart')
        time.sleep(2)
        assert len(self.cards())==count_after_unknown,'restart blindly resent unknown platform result'
        self.operation += '-unknown'
        self.command(a,'begin-active',b,expected_code=409)
        self.event('unknown',status='PASS',result='not declared delivered; release blocked; no blind resend')
        self.event('full_contract',status='BLOCKED',reason='c62 bootstrap and ambiguous same-start A-B-A require separate solution')

    def synchronous_compatibility(self):
        self.start('legacy-api',old='rc7',profiles=False)
        self.start('api-active',profiles=False)
        self.start('api-standby',mode='standby',profiles=False)
        rows=[]
        for api in ('verify','notifications'):
            for scenario in ('success','failure','slow'):
                body=dict(receiver=dict(metadata=dict(name='inline',labels=dict(type='global')),
                                        spec=dict(webhook=dict(url='http://mock:8080/sync/'+scenario))),
                          alert=dict(alerts=[dict(labels=dict(alertname='synthetic-api'))]))
                old=self.request('legacy-api:19094','/api/v2/'+api,body)
                new=self.request('api-active:19094','/api/v2/'+api,body)
                want=200 if scenario=='success' else 400
                assert old[0]==new[0]==want,(api,scenario,old,new)
                if want==200:
                    assert old[1]==new[1],(old,new)
                standby=self.request('api-standby:19094','/api/v2/'+api,body)
                assert standby[0]==503,standby
                rows.append(dict(api=api,scenario=scenario,old_status=old[0],new_status=new[0],standby_status=503))
        self.event('real_binary_synchronous_compatibility',status='PASS',cases=rows,
                   original_rc7_binary_sha256='ac28d0c192d2b1ba043b57e0876b3fbc558538bd1c908b2899f23bc7db0b6b62',
                   worker_timeout_millis=200,timeout_is_explicit_fixture_setting=True,
                   caller_result_preserved=True,standby_not_falsely_acknowledged=True)

    def real_am_peers(self, target):
        self.request('mock:8080','/control/mode',{'target':target})
        config = '''route:
  receiver: fixture
  group_by: [alertname]
  group_wait: 200ms
  group_interval: 1s
  repeat_interval: 1h
receivers:
- name: fixture
  webhook_configs:
  - url: http://mock:8080/ingress
    send_resolved: true
'''
        (self.root/'am.yml').write_text(config)
        for alias,peer in [('am-peer-a','am-peer-b'),('am-peer-b','am-peer-a')]:
            name=self.prefix+'-'+alias
            self.names.append(name)
            self.docker('run','-d','--pull','never','--name',name,'--network',self.net,'--network-alias',alias,
                        '-v',str(self.root/'am.yml')+':/etc/alertmanager/fixture.yml:ro',
                        'prom/alertmanager:v0.23.0','--config.file=/etc/alertmanager/fixture.yml',
                        '--cluster.peer='+peer+':9094','--cluster.settle-timeout=2s')
            self.wait(lambda:self.request(alias+':9093','/-/ready')[0]==200,label=alias+' Ready')
        time.sleep(3)
        n=len(self.cards())
        now=dt.datetime.now(dt.timezone.utc)
        alert=[dict(labels=dict(alertname='real-am-peers',severity='critical',cluster='fixture',namespace='fixture'),
                    annotations=dict(summary='synthetic AM source'),startsAt=now.isoformat(),
                    endsAt=(now+dt.timedelta(minutes=10)).isoformat())]
        for alias in ('am-peer-a','am-peer-b'):
            assert self.request(alias+':9093','/api/v2/alerts',alert)[0]==200
        self.count(n+1)
        time.sleep(2)
        assert len(self.cards())==n+1,'AM peers produced duplicate base card'
        self.event('real_am_two_peer',status='PASS',platform_cards=1,am_version='0.23.0',
                   service_fixture=True,kubernetes_service_propagation_tested=False)

    def finish(self, error=None):
        self.stop_sampling.set()
        if hasattr(self,'sampler'):
            self.sampler.join(timeout=5)
        (self.root/'resource-samples.json').write_text(json.dumps(self.samples,indent=2)+'\n')
        stats=self.docker('stats','--no-stream','--format','{{json .}}',*self.names,check=False)
        (self.root/'docker-stats.jsonl').write_text(stats.stdout)
        for name in self.names:
            log=self.docker('logs',name,check=False)
            (self.root/(name+'.log')).write_text(log.stdout+log.stderr)
        volumes={p.parent.name:p.stat().st_size for p in self.root.glob('*/spool.db')}
        result=dict(protocol_development='FAIL' if error else 'PASS_WITH_BLOCKED_GATES',
                    scenario=self.args.scenario,
                    duration_seconds=round(time.monotonic()-self.begin,3),error=str(error) if error else None,
                    events=self.events,bolt_file_bytes=volumes,
                    production_or_uat_access=False,external_network=False)
        (self.root/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        if not self.args.keep:
            for name in reversed(self.names):
                self.docker('rm','-f',name,check=False)
            self.docker('network','rm',self.net,check=False)
        print(json.dumps(result,indent=2))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary',required=True,type=Path,help='CGO=0 linux/amd64 candidate executable')
    parser.add_argument('--output',required=True,type=Path,help='new synthetic evidence directory')
    parser.add_argument('--keep',action='store_true',help='retain only this fixture containers for debugging')
    parser.add_argument('--scenario',choices=('full','sync'),default='full',help='focused API compatibility or complete protocol fixture')
    args=parser.parse_args()
    fixture=Fixture(args)
    error=None
    try:
        if args.scenario=='sync':
            fixture.prepare()
            fixture.synchronous_compatibility()
        else:
            fixture.execute()
    except Exception as caught:
        error=caught
    finally:
        fixture.finish(error)
    if error:
        raise SystemExit(str(error))


if __name__=='__main__':
    main()
