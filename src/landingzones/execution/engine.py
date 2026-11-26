"""Independent connection execution with portable labels and local receipts."""
from contextlib import contextmanager
import copy
from dataclasses import replace
from datetime import datetime, timezone
import fcntl
import hashlib
import os
import json
from pathlib import Path
import warnings

from landingzones.transfer_events import EVENT_HEADER, create_transfer_event, event_to_tsv_row, new_identifier
from .adapters import adapter, manifest, matches
from .model import Step, load_settings, load_steps, local_root, name, resolve_local_endpoint
from .package import LABEL, admit
from .source import Source
from .progress import Progress, phase_heartbeat
from .storage import StateStore


def now():
    return datetime.now(timezone.utc).isoformat()


def _matches_receipt(existing, plan):
    keys = [plan['receipt_key']]
    if plan['previous_copy_key'] is not None and existing['status'] == 'completed':
        keys.append(plan['previous_copy_key'])
    previous_key = existing.get('receipt_key')
    if previous_key in keys:
        return True
    if (not previous_key or plan['connection']['verification'] != 'size'
            or not matches(existing['manifest'], plan['manifest'], 'size')):
        return False
    # The change from checksums to sizes can change the version of an unlabelled
    # retained source. Do not publish it again under a different request key.
    previous = json.loads(previous_key)
    for key in keys:
        candidate = json.loads(key)
        candidate[6] = previous[6]
        if candidate == previous:
            return True
    return False


class Executor:
    def __init__(self, config_file):
        self.settings = load_settings(config_file)
        self.store = StateStore(self.settings['state_dir'], self.settings['max_concurrent_transfers'])
        if self.store.root.exists():
            for path in self.store.root.glob('*.json'):
                if json.loads(path.read_text()).get('schema_version') != 2:
                    raise ValueError('Legacy request state needs explicit migration; use a separate state directory')

    @staticmethod
    def _connection(request):
        group = request.get('connection')
        if 'deliveries' in request:
            if group or not isinstance(request['deliveries'], list) or len(request['deliveries']) != 1 or set(request['deliveries'][0]) != {'flow_group'}:
                raise ValueError('Select exactly one connection; coordinated recipients are deferred')
            group = request['deliveries'][0]['flow_group']
        if not isinstance(group, str) or not group:
            raise ValueError('Unknown configured connection')
        return group

    @contextmanager
    def _route(self, connection, *, step=None, intake_source=None):
        # The route lock stabilizes its outstanding receipts while we also guard
        # their recorded endpoints; a prior configuration or intake can differ.
        with self.store.locked(connection):
            pending = []
            endpoints = {step.source, step.destination} if step is not None else set()
            if intake_source is not None:
                endpoints.add(str(Path(local_root(intake_source)).parent))
            for path in sorted(self.store.root.glob('*.json')):
                state = json.loads(path.read_text())
                if state['status'] != 'completed' and state['deliveries'][0]['flow_group'] == connection:
                    pending.append(state)
                    for work in self._work(state):
                        endpoints.update((work['definition']['source'], work['definition']['destination']))
            with self.store.queues(endpoints):
                yield pending

    def preflight(self, request):
        return self._preflight(request)

    def _preflight(self, request, observed=None):
        if not isinstance(request, dict) or set(request) - {'idempotency_key', 'payload_name', 'connection', 'deliveries', 'intake', 'itinerary', 'adapter_options'}:
            raise ValueError('Unsupported request fields')
        key = request.get('idempotency_key')
        if not isinstance(key, str) or not key:
            raise ValueError('idempotency_key is required')
        payload = name(request['payload_name'])
        group = self._connection(request)
        groups = load_steps(self.settings)
        if group not in groups:
            raise ValueError('Unknown configured connection')
        step = groups[group][0]
        remote_destination = step.destination.startswith(('ssh://', 'sftp://', 'ena://'))
        if remote_destination and step.operation == 'move' and not step.sftp_publication:
            raise ValueError('Remote destination move requires durable publication support; use copy or a legacy route')
        if step.adapter == 'sftp' and step.operation == 'move' and step.source.startswith(('ssh://', 'sftp://')):
            raise ValueError('SFTP destination moves require a local source')
        if not remote_destination:
            local_root(step.destination)
            if not Path(step.destination).is_dir():
                raise ValueError('Destination root must exist')
        intake = request.get('intake')
        intake_step = None
        admission_step = step
        if intake:
            if not isinstance(intake, dict) or set(intake) != {'source_path', 'input_root', 'operation'}:
                raise ValueError('Intake requires source_path, input_root and operation')
            if (step.source.startswith(('ssh://', 'sftp://'))
                    or Path(resolve_local_endpoint(intake['input_root'], self.settings.get('local_path_base'))) != Path(step.source)
                    or intake['operation'] not in ('copy', 'move')):
                raise ValueError('Intake must match a configured same-server input')
            source = Path(local_root(intake['source_path']))
            if source.name != payload:
                raise ValueError('Intake folder basename must equal payload_name')
            if source != Path(step.source) / payload:
                intake_step = replace(step, identifiers='intake:' + step.identifiers, source=str(source.parent), destination=step.source,
                                      adapter='local', operation=intake['operation'], readiness_policy='producer_marker',
                                      publication_group='', publication_directory_mode='')
                admission_step = intake_step
        for work in ([intake_step] if intake_step else []) + [step]:
            if not work.source.startswith(('ssh://', 'sftp://')) and not work.destination.startswith(('ssh://', 'sftp://', 'ena://')):
                source = Path(local_root(work.source)) / payload
                destination = Path(local_root(work.destination)) / payload
                if source == destination or source in destination.parents or destination in source.parents:
                    raise ValueError('Source and destination overlap')
        options = request.get('adapter_options', {})
        if not isinstance(options, dict):
            raise ValueError('adapter_options must be an object')
        transport = adapter(step, self.settings)  # Construction never opens a connection.
        if options and not hasattr(transport, 'prepare_request'):
            raise ValueError('This adapter does not support request options')
        if observed is None:
            with Source(admission_step, self.settings) as source:
                if not source.ready(payload):
                    raise ValueError('Producer completion marker .ready is required')
                contents, previous = source.inspect(payload), source.label(payload)
        else:
            # Discovery already read this inventory and label while holding the
            # route guards. Transfer checks the source again before sending bytes.
            contents, previous = observed
        label = admit(contents, previous, step.admission_policy, request.get('itinerary'),
                      verification=step.verification)
        adapter_request = transport.prepare_request(options, contents) if hasattr(transport, 'prepare_request') else {}
        # An unlabelled package has a stable local admission key until it receives a label.
        identity = previous.get('package_id') if isinstance(previous, dict) and previous.get('package_id') == label['package_id'] else None
        receipt_parts = [step.runtime_id, step.identifiers, step.source, step.destination, step.operation,
                         identity, label['content_version'], payload]
        # Retained copies already delivered by an ungrouped route must not be
        # submitted again just because new deliveries now inherit a group.
        previous_copy_key = None
        if step.operation == 'copy' and step.publication_directory_mode and not adapter_request:
            previous_copy_key = json.dumps(receipt_parts, sort_keys=True)
        if step.publication_group:
            receipt_parts.append({'publication_group': step.publication_group})
        # Preserve existing receipt identities for routes without external submissions.
        if adapter_request:
            receipt_parts.append(adapter_request)
        receipt_key = json.dumps(receipt_parts, sort_keys=True)
        return {'payload_name': payload, 'manifest': contents, 'label': label,
                'receipt_key': receipt_key, 'previous_copy_key': previous_copy_key,
                'connection': step.record(), 'adapter_request': adapter_request,
                'intake': intake_step.record() if intake_step else None}

    def submit(self, request):
        if self.store.limit == 1:
            with self.store.locked():
                return self._submit(request)
        group = self._connection(request)
        step = load_steps(self.settings).get(group, [None])[0]
        intake = request.get('intake') or {}
        if not isinstance(intake, dict):
            raise ValueError('Invalid intake')
        with self._route(group, step=step, intake_source=intake.get('source_path')):
            return self._submit(request)

    def _submit(self, request, observed=None):
        serialized = json.dumps(request, sort_keys=True)
        key = request.get('idempotency_key')
        if not isinstance(key, str) or not key:
            raise ValueError('idempotency_key is required')
        request_id = hashlib.sha256(key.encode()).hexdigest()
        if self.store.path(request_id).exists():
            state = self.store.read(request_id)
            self._validate_state(state)
            if state['request_content'] != serialized:
                raise ValueError('Idempotency key was already used for different content')
        else:
            plan = self._preflight(request, observed)
            for path in self.store.root.glob('*.json'):
                existing = json.loads(path.read_text())
                if _matches_receipt(existing, plan):
                    self._validate_state(existing)
                    return self._execute(existing)
            def work(definition):
                return {'definition': definition, 'step_id': new_identifier(), 'status': 'pending', 'attempts': [],
                        'attempt_limit': {phase: definition['max_attempts'] for phase in ('transfer', 'cleanup', 'promotion', 'submission')}}
            state = {'schema_version': 2, 'request_id': request_id, 'request_content': serialized,
                     'receipt_key': plan['receipt_key'], 'run_id': plan['label']['transfer_run_id'],
                     'payload_id': plan['label']['package_id'], 'payload_version': plan['label']['content_version'],
                     'payload_name': plan['payload_name'], 'manifest': plan['manifest'], 'label': plan['label'],
                     'adapter_request': plan['adapter_request'],
                     'total_bytes': sum(v.get('size', 0) for v in plan['manifest'].values()),
                     'context': self.settings['execution_context'], 'status': 'accepted', 'accepted_at': now(),
                     'intake': work(plan['intake']) if plan['intake'] else None,
                     'deliveries': [{'flow_group': plan['connection']['flow_group'], 'status': 'pending',
                                     'steps': [work(plan['connection'])]}]}
            try:
                self.store.create(state)
            except FileExistsError as exc:
                raise ValueError('Idempotency key was already used for different content') from exc
        return self._execute(state)

    def _validate_state(self, state):
        if state.get('schema_version') != 2:
            raise ValueError('Legacy request state needs explicit migration; use a separate state directory')
        if state['context'] != self.settings['execution_context']:
            raise ValueError('Request belongs to another execution context')

    def status(self, request_id):
        state = self.store.read(request_id)
        self._validate_state(state)
        end = state.get('completed_at', now())
        state['elapsed_seconds'] = (datetime.fromisoformat(end) - datetime.fromisoformat(state['accepted_at'])).total_seconds()
        return state

    def resume(self, request_id, retry_parked=False):
        recorded = self.store.read(request_id)
        self._validate_state(recorded)
        connection = recorded['deliveries'][0]['flow_group']
        with self.store.locked(connection):
            state = self.store.read(request_id)
            if state['deliveries'][0]['flow_group'] != connection:
                raise ValueError('Recorded route changed before resume')
            endpoints = [work['definition'][side] for work in self._work(state)
                         for side in ('source', 'destination')]
            with self.store.queues(endpoints):
                return self._resume(state, retry_parked)

    def _resume(self, state, retry_parked=False):
        self._validate_state(state)
        if retry_parked:
            for work in self._work(state):
                if work['status'] == 'parked':
                    work['status'] = 'pending'
                    for phase in work['attempt_limit']:
                        work['attempt_limit'][phase] = sum(a['phase'] == phase for a in work['attempts']) + work['definition']['max_attempts']
            self.store.save(state)
        return self._execute(state)

    def discover(self, connection):
        groups = load_steps(self.settings)
        if connection not in groups:
            raise ValueError('Unknown configured connection')
        step = groups[connection][0]
        with self._route(connection, step=step) as pending:
            return self._discover(step, pending)

    def _discover(self, step, pending):
        connection = step.flow_group
        results = [self._resume(state) for state in pending]
        with Source(step, self.settings) as source:
            candidates = source.names()
            resumed = {r['payload_name'] for r in results}
            for payload in candidates:
                if payload in resumed:
                    continue
                if not source.ready(payload):
                    results.append({'payload_name': payload, 'status': 'waiting', 'reason': 'producer readiness'})
                    continue
                from .package import version
                contents = source.inspect(payload)
                fingerprint = version(contents)
                label = source.label(payload)
                identity = label.get('package_id', '') if isinstance(label, dict) else ''
                token = json.dumps([step.record(), payload, fingerprint, identity], sort_keys=True)
                request = {'idempotency_key': 'discovery:' + hashlib.sha256(token.encode()).hexdigest(),
                           'payload_name': payload, 'connection': connection}
                try:
                    results.append(self._submit(request, (contents, label)))
                except (ValueError, OSError) as exc:
                    results.append({'payload_name': payload, 'status': 'blocked', 'reason': str(exc)})
        return {'connection': connection, 'results': results,
                'status': 'blocked' if any(r['status'] in ('blocked', 'parked') for r in results) else 'completed'}

    def reconcile(self, request_id, receipt_file, environment):
        """Import independently obtained submission evidence; never contact a service."""
        with self.store.locked():
            state = self.store.read(request_id)
            self._validate_state(state)
            work = state['deliveries'][0]['steps'][0]
            plan = state.get('adapter_request', {})
            if not work.get('published') or not plan or not work.get('submission'):
                raise ValueError('Only an attempted submission after delivery can be reconciled')
            if environment != plan.get('environment'):
                raise ValueError('Receipt environment must match the accepted submission')
            transport = adapter(Step(**work['definition']), self.settings)
            if not hasattr(transport, 'reconcile'):
                raise ValueError('This adapter does not support receipt reconciliation')
            with open(receipt_file, 'rb') as handle:
                raw = handle.read(4 * 1024 * 1024 + 1)
            if len(raw) > 4 * 1024 * 1024:
                raise ValueError('Submission receipt is too large')
            try:
                receipt_xml = raw.decode('utf-8')
            except UnicodeDecodeError:
                raise ValueError('Submission receipt must be UTF-8') from None
            result = transport.reconcile(plan, receipt_xml, work['submission'], lambda: self.store.save(state))
            work.update(result=result, status='completed', completed_at=now(),
                        reconciled_at=now(), receipt_sha256=hashlib.sha256(raw).hexdigest())
            self.store.save(state)
            attempt = next(a for a in reversed(work['attempts']) if a['phase'] == 'submission')
            self._event(state, work, attempt, 'completed', 'submission', 'Receipt reconciled locally')
            return self._execute(state)

    @staticmethod
    def _work(state):
        return ([state['intake']] if state['intake'] else []) + state['deliveries'][0]['steps']

    def _event(self, state, work, attempt, status, phase, message='', progress=None, heartbeat=False):
        step = Step(**work['definition'])
        details = {'request_id': state['request_id'], 'package_id': state['payload_id'],
                   'content_version': state['payload_version'], 'detail': message}
        if progress is not None:
            details['progress'] = progress
        if heartbeat:
            details['heartbeat'] = True
        event = create_transfer_event(step.identifiers, step.system, step.runtime_id, step.users, status, phase,
            event_time_utc=datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z'),
            run_id=state['run_id'], attempt_id=attempt['attempt_id'], run_name=state['payload_name'],
            flow_group=step.flow_group, source_path=step.source, destination_path=step.destination,
            message=json.dumps(details))
        try:
            spool = Path(self.settings['event_spool'])
            spool.parent.mkdir(parents=True, exist_ok=True)
            with spool.open('a') as handle:
                fcntl.flock(handle, fcntl.LOCK_EX)
                if os.fstat(handle.fileno()).st_size == 0:
                    handle.write(EVENT_HEADER + '\n')
                handle.write(event_to_tsv_row(event) + '\n')
        except OSError as exc:
            warnings.warn('Transfer Event append failed: ' + str(exc))

    def _phase(self, state, work, phase, action):
        used = sum(a['phase'] == phase for a in work['attempts'])
        if used >= work['attempt_limit'][phase]:
            work['status'] = 'parked'
            self.store.save(state)
            return False
        attempt = {'attempt_id': new_identifier(), 'phase': phase, 'status': 'started', 'started_at': now()}
        work['attempts'].append(attempt)
        self.store.save(state)
        start = None
        if phase == 'transfer':
            start = Progress.totals(state)
        self._event(state, work, attempt, 'started', phase, progress=start)
        try:
            with phase_heartbeat(lambda: self._event(state, work, attempt, 'started', phase, heartbeat=True)):
                action()
            attempt.update(status='completed', finished_at=now())
            attempt['elapsed_seconds'] = (datetime.fromisoformat(attempt['finished_at']) - datetime.fromisoformat(attempt['started_at'])).total_seconds()
            self.store.save(state)
            return True
        except Exception as exc:
            attempt.update(status='failed', finished_at=now(), error=str(exc),
                           retryable=not isinstance(exc, (ValueError, FileExistsError)))
            work['status'] = 'parked' if not attempt['retryable'] or used + 1 >= work['attempt_limit'][phase] else 'failed'
            self.store.save(state)
            self._event(state, work, attempt, 'failed', phase, str(exc))
            return False

    def _execute_work(self, state, work):
        accepted = (work.get('published') and state.get('adapter_request')
                    and work.get('submission', {}).get('status') == 'accepted')
        if work['status'] == 'completed' or (work['status'] == 'parked' and not accepted):
            return
        step = Step(**work['definition'])
        source_step = step
        if step.verification == 'size' and any('sha256' in entry for entry in state['manifest'].values()):
            # Older size-copy receipts still accepted a full source checksum manifest.
            source_step = replace(step, verification='checksum')
        payload = state['payload_name']
        remote_move = step.sftp_publication and step.operation == 'move'
        # Each phase opens its own transport so connectivity failures are recorded.
        def transfer():
            with adapter(step, self.settings) as transport, Source(source_step, self.settings) as source:
                transport.execution_lock = self.store.execution_lock
                attempt = work['attempts'][-1]
                progress = Progress(state, lambda sample: self._event(
                    state, work, attempt, 'started', 'transfer', progress=sample))
                transport.progress = progress
                if step.rsync_source_pull:
                    source.transport.progress = progress
                stage = transport.stage_name(work['step_id'])
                if getattr(transport, 'publication_mode', 'rename') == 'rename' and transport.exists(payload):
                    raise FileExistsError('Destination conflict')
                if source.inspect(payload) != state['manifest']:
                    raise ValueError('Source changed since acceptance')
                cache = self.store.root / 'source-cache' / work['step_id']
                if step.rsync_source_pull:
                    with transport.staging_directory(stage) as path:
                        source.transport.pull_directory(payload, path, state['manifest'], self.store.execution_lock)
                elif step.rsync_destination_push:
                    transport.push_directory(source.root / payload, stage, state['manifest'], self.store.execution_lock)
                else:
                    local = source.materialize(payload, cache, state['manifest'])
                    transport.copy(local, stage, state['manifest'])
                if source.inspect(payload) != state['manifest']:
                    raise ValueError('Source changed during transfer')
                if not matches(transport.inspect(stage), state['manifest'], step.verification):
                    raise ValueError('Staging verification failed')
                label = copy.deepcopy(state['label'])
                label['history'].append({'connection': step.flow_group, 'transfer_id': state['request_id'],
                                         'staged_at': now(), 'event': 'handoff_prepared'})
                itinerary = label.get('itinerary', [])
                observed = [h.get('connection') for h in label['history'] if isinstance(h, dict)]
                work['itinerary_status'] = ('unspecified' if not itinerary else
                    'on_plan' if observed == itinerary[:len(observed)] else 'discrepancy')
                transport.write_label(stage, label)
                if step.publication_group and not step.sftp_publication:
                    transport.prepare_publication(stage, state['manifest'])
                work['output_label'] = label
                if cache.exists():
                    import shutil
                    shutil.rmtree(cache)
                progress.verified()
                work['staged'] = True
                work['status'] = 'staged'
                self.store.save(state)
        if not work.get('staged') and not self._phase(state, work, 'transfer', transfer):
            return
        def clean():
            with adapter(step, self.settings) as transport, Source(source_step, self.settings) as source:
                stage = transport.stage_name(work['step_id'])
                if not matches(transport.inspect(stage), state['manifest'], step.verification):
                    raise ValueError('Staging changed; cleanup refused')
                if step.publication_group:
                    transport.prepare_publication(stage, state['manifest'])
                if hasattr(transport, 'check_move_publication'):
                    transport.check_move_publication(stage, payload)
                source.remove(payload, state['manifest'])
                work['cleaned'] = True
                self.store.save(state)
        if step.operation == 'move' and not remote_move and not work.get('cleaned') and not self._phase(state, work, 'cleanup', clean):
            return
        def publish():
            with adapter(step, self.settings) as transport:
                transport.execution_lock = self.store.execution_lock
                stage = transport.stage_name(work['step_id'])
                rename_publication = getattr(transport, 'publication_mode', 'rename') == 'rename'
                if transport.exists(stage):
                    if rename_publication and transport.exists(payload):
                        raise FileExistsError('Destination conflict')
                    if step.batched_publication:
                        transport.prepare_verified_publication(stage, state['manifest'], work['output_label'])
                    else:
                        if not matches(transport.inspect(stage), state['manifest'], step.verification):
                            raise ValueError('Staging changed; publication refused')
                        if step.publication_group or remote_move:
                            transport.prepare_publication(stage, state['manifest'])
                    work['promotion_intent'] = True
                    self.store.save(state)
                    receipt = transport.promote(stage, payload)
                    if receipt is not None:
                        work['delivery_receipt'] = receipt
                elif not rename_publication:
                    raise ValueError('Verified upload directory is missing; completion cannot be inferred')
                elif not work.get('promotion_intent'):
                    raise ValueError('Private staging missing without publication intent')
                elif remote_move:
                    # Losing a rename acknowledgement is ambiguous if the consumer
                    # already collected the output. Retain the source for reconciliation.
                    if not transport.exists(payload) or transport.inspect(payload) != state['manifest']:
                        raise ValueError('Remote delivery cannot be confirmed after interruption; source retained')
                    with transport.sftp.open(transport.path(payload) + '/' + LABEL, 'rb') as label:
                        if json.load(label) != work['output_label']:
                            raise ValueError('Remote publication label changed; source retained')
                if remote_move:
                    transport.confirm_publication(stage)
                # Only this executor may remove its private stage. With durable intent,
                # its disappearance proves rename, even if the consumer removed output.
                work.update(status='published', published=True, verified=True)
                self.store.save(state)
        if not work.get('published'):
            if not self._phase(state, work, 'promotion', publish):
                return
            attempt = work['attempts'][-1]
            self._event(state, work, attempt, 'delivered', 'promotion')
        if remote_move and not work.get('cleaned'):
            def clean_published():
                with Source(source_step, self.settings) as source:
                    source.remove(payload, state['manifest'])
                work['cleaned'] = True
                self.store.save(state)
            if not self._phase(state, work, 'cleanup', clean_published):
                return
        plan = state.get('adapter_request', {}) if work is not state.get('intake') else {}
        if plan and work.get('submission', {}).get('status') == 'accepted':
            # The service receipt can be durable even if the process stopped before
            # phase bookkeeping. Recover it without spending a retry or another POST.
            work['result'] = work['submission']['result']
        elif plan:
            def finalize():
                transport = adapter(step, self.settings)
                if not hasattr(transport, 'finalize'):
                    raise ValueError('Adapter no longer supports the accepted submission')
                progress = work.setdefault('submission', {})
                work['result'] = transport.finalize(plan, work.get('delivery_receipt', {}), progress,
                                                    lambda: self.store.save(state))
            if not self._phase(state, work, 'submission', finalize):
                return
        work.update(status='completed', completed_at=now())
        self.store.save(state)
        self._event(state, work, work['attempts'][-1], 'completed', 'submission' if plan else 'cleanup')

    def _execute(self, state):
        if state['status'] == 'completed':
            return state
        for work in self._work(state):
            self._execute_work(state, work)
            if work['status'] != 'completed':
                state['status'] = 'blocked'
                state['deliveries'][0]['status'] = 'blocked'
                self.store.save(state)
                return state
            if work is state['intake']:
                state['label'] = work['output_label']
        state['deliveries'][0]['status'] = 'completed'
        state.update(status='completed', completed_at=now())
        self.store.save(state)
        return state
