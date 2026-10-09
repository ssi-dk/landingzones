"""External submission is an observable phase after delivery, never a new transfer."""
from dataclasses import replace
import uuid

import pytest

from landingzones.monitoring import ingest_event_spool, query_run_detail, query_run_summaries
from landingzones.monitoring_service import MonitoringApplication
from landingzones.transfer_events import (
    EVENT_HEADER,
    create_transfer_event,
    event_from_tsv_row,
    event_to_tsv_row,
)


def event(status='started', phase='submission', **values):
    defaults = dict(run_id=str(uuid.uuid4()), attempt_id=str(uuid.uuid4()))
    defaults.update(values)
    return create_transfer_event('external', 'example', 'test', 'operator', status, phase, **defaults)


@pytest.mark.parametrize('status', ['started', 'failed', 'completed'])
def test_submission_event_has_an_explicit_versioned_round_trip(status):
    value = event(status)
    assert value.schema_version == '2'
    assert event_from_tsv_row(event_to_tsv_row(value)) == value


def test_existing_transfer_events_remain_schema_one():
    value = event(phase='transfer')
    assert value.schema_version == '1'
    assert event_from_tsv_row(event_to_tsv_row(value)) == value


def test_submission_phase_cannot_be_disguised_as_schema_one():
    value = replace(event(), schema_version='1')
    with pytest.raises(ValueError, match='phase'):
        event_to_tsv_row(value)


@pytest.mark.parametrize('field', ['run_id', 'attempt_id'])
def test_failed_submission_requires_run_and_attempt_identity(field):
    with pytest.raises(ValueError, match=field):
        event('failed', **{field: None})


def test_mixed_spool_preserves_delivered_state_through_submission_retry(tmp_path):
    database_url = 'sqlite:///' + str(tmp_path / 'events.sqlite')
    spool = tmp_path / 'events.tsv'
    spool.write_text(EVENT_HEADER + '\n')
    run_id = str(uuid.uuid4())
    first_attempt = str(uuid.uuid4())
    retry_attempt = str(uuid.uuid4())
    sequence = [
        ('delivered', 'promotion', first_attempt, 'delivered with cleanup pending'),
        ('started', 'submission', first_attempt, 'delivered with submission pending'),
        ('failed', 'submission', first_attempt, 'delivered with submission failed'),
        ('started', 'submission', retry_attempt, 'delivered with submission pending'),
        ('completed', 'submission', retry_attempt, 'completed'),
    ]
    for index, (status, phase, attempt, state) in enumerate(sequence):
        value = event(status, phase, run_id=run_id, attempt_id=attempt,
                      event_time_utc='2026-10-09T12:00:0{}Z'.format(index),
                      directory='/example/input/package')
        with spool.open('a') as handle:
            handle.write(event_to_tsv_row(value) + '\n')
        result = ingest_event_spool(database_url, spool)
        assert result.inserted == 1 and result.skipped == 0
        summary = query_run_summaries(database_url)[0]
        assert summary['state'] == state
        assert summary['current_phase'] == phase
        response_status, _, html = MonitoringApplication(database_url).respond('/', '')
        assert response_status == 200 and state in html
        if phase == 'submission' and status in ('started', 'failed'):
            expected_class = 'progress-failed' if status == 'failed' else 'progress-active'
            assert 'class="{}"'.format(expected_class) in html
            assert ('{} (submission)'.format(state) if status == 'started'
                    else '{} at submission'.format(state)) in html
    replay = ingest_event_spool(database_url, spool, spool_id='replay')
    assert replay.inserted == 0 and replay.duplicates == len(sequence)
    assert [row['schema_version'] for row in query_run_detail(database_url, run_id)['timeline']] == [
        '1', '2', '2', '2', '2',
    ]


def test_future_schema_stops_ingestion_without_advancing_checkpoint(tmp_path):
    from landingzones.monitoring import UnsupportedEventSpool

    database_url = 'sqlite:///' + str(tmp_path / 'events.sqlite')
    spool = tmp_path / 'events.tsv'
    run_id = str(uuid.uuid4())
    first = event('delivered', 'promotion', run_id=run_id,
                  event_time_utc='2026-10-09T12:00:00Z')
    next_event = event(run_id=run_id, event_time_utc='2026-10-09T12:00:01Z')
    future_event = event('failed', run_id=run_id, event_time_utc='2026-10-09T12:00:02Z')
    spool.write_text(EVENT_HEADER + '\n' + event_to_tsv_row(first) + '\n')
    original = ingest_event_spool(database_url, spool)
    future_row = event_to_tsv_row(future_event).replace('2\t', '3\t', 1)
    with spool.open('a') as handle:
        handle.write(event_to_tsv_row(next_event) + '\n')
        handle.write(future_row + '\n')
    with pytest.raises(UnsupportedEventSpool, match='schema version: 3'):
        ingest_event_spool(database_url, spool)
    assert len(query_run_detail(database_url, run_id)['timeline']) == 1
    # Simulate a corrected supported producer row of the same size, then resume
    # from the original checkpoint. An incompatible reader must not consume it.
    spool.write_text(spool.read_text().replace(future_row, event_to_tsv_row(future_event)))
    recovered = ingest_event_spool(database_url, spool)
    assert recovered.inserted == 2
    assert recovered.checkpoint_offset > original.checkpoint_offset
