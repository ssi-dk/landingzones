"""Portable identity is independent of local connection receipts and label format."""
import hashlib
import json
import uuid

LABEL = '.landingzones-package.json'


def is_metadata(path):
    return path in ('.ready', LABEL, '.landing_zones') or path.startswith('.landing_zones/')


def version(contents):
    return hashlib.sha256(json.dumps(contents, sort_keys=True).encode()).hexdigest()


def admit(contents, previous=None, policy='strict', itinerary=None):
    fingerprint = version(contents)
    valid = (isinstance(previous, dict) and previous.get('schema_version') in (0, 1)
             and isinstance(previous.get('package_id'), str) and previous['package_id']
             and previous.get('content_version') == fingerprint
             and previous.get('manifest') == contents)
    if previous is not None and not valid and policy != 'relabel':
        raise ValueError('Unverifiable or changed package label; explicit relabel admission is required')
    if valid:
        # Only understood fields are promoted; retain the old label as evidence.
        label = {key: previous[key] for key in ('package_id', 'transfer_run_id', 'parent_package_ids', 'sequencing_run_id', 'itinerary', 'history') if key in previous}
        if 'sequencing_run_id' in label and not isinstance(label['sequencing_run_id'], str):
            raise ValueError('Invalid sequencing run identity')
        if not isinstance(label.get('parent_package_ids', []), list) or not all(isinstance(x, str) and x for x in label.get('parent_package_ids', [])):
            raise ValueError('Invalid parent package identities')
        if not isinstance(label.get('history', []), list):
            raise ValueError('Invalid package history')
        if not isinstance(label.get('itinerary', []), list) or not all(isinstance(x, str) for x in label.get('itinerary', [])):
            raise ValueError('Invalid descriptive itinerary')
    else:
        label = {'package_id': str(uuid.uuid4())}
    label.update(schema_version=1, content_version=fingerprint, manifest=contents)
    if previous is not None and (not valid or previous.get('schema_version') != 1):
        label['source_metadata'] = previous
    elif valid and 'source_metadata' in previous:
        label['source_metadata'] = previous['source_metadata']
    if itinerary is not None:
        if not isinstance(itinerary, list) or not all(isinstance(x, str) for x in itinerary):
            raise ValueError('Itinerary must be a list of connection identifiers')
        label['itinerary'] = itinerary
    if 'transfer_run_id' in label and (not isinstance(label['transfer_run_id'], str) or not label['transfer_run_id']):
        raise ValueError('Invalid Transfer Run identity')
    label.setdefault('transfer_run_id', str(uuid.uuid4()))
    label.setdefault('history', [])
    return label
