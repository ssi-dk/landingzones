"""Configured source access; remote sources are pulled by the local executor."""
from dataclasses import replace
import errno
import json
from pathlib import Path
import stat

from .adapters import SFTPAdapter, manifest, cleanup
from .model import local_root, name
from .package import LABEL, is_metadata


class Source:
    def __init__(self, step, settings):
        self.step = step
        self.remote = step.source.startswith(('ssh://', 'sftp://'))
        self.transport = SFTPAdapter(replace(step, destination=step.source, verification='checksum'), settings) if self.remote else None
        self.root = None if self.remote else Path(local_root(step.source))

    def __enter__(self):
        if self.transport:
            self.transport.__enter__()
        return self

    def __exit__(self, *args):
        if self.transport:
            self.transport.__exit__(*args)

    def names(self):
        if self.remote:
            return sorted(name(e.filename) for e in self.transport.sftp.listdir_attr(self.transport.root)
                          if stat.S_ISDIR(e.st_mode) and not e.filename.startswith('.'))
        return sorted(name(p.name) for p in self.root.iterdir() if p.is_dir() and not p.is_symlink() and not p.name.startswith('.'))

    def ready(self, payload):
        if self.step.readiness_policy == 'managed_ready':
            return True  # This input is exclusively published by a trusted producer.
        if self.remote:
            try:
                mode = self.transport.check_path(self.transport.path(payload + '/.ready')).st_mode
                return stat.S_ISREG(mode)
            except IOError as exc:
                if exc.errno == errno.ENOENT:
                    return False
                raise
        marker = self.root / payload / '.ready'
        return marker.is_file() and not marker.is_symlink()

    def inspect(self, payload):
        archive = payload + '/.landing_zones/landingzone-run-archive.tar'
        if (self.transport.exists(archive) if self.remote else (self.root / archive).exists()):
            raise ValueError('Legacy archive packages must remain on legacy routes or be unpacked before admission')
        return self.transport.inspect(payload) if self.remote else manifest(self.root / payload)

    def read(self, relative):
        if self.remote:
            if not self.transport.exists(relative):
                return None
            if not stat.S_ISREG(self.transport.check_path(self.transport.path(relative)).st_mode):
                raise ValueError('Label must be a regular file')
            with self.transport.sftp.open(self.transport.path(relative), 'rb') as handle:
                return handle.read().decode('utf-8')
        path = self.root / relative
        if path.is_symlink():
            raise ValueError('Label symlink refused')
        return path.read_text() if path.exists() else None

    def label(self, payload):
        raw = self.read(payload + '/' + LABEL)
        if raw is not None:
            try:
                value = json.loads(raw)
                return value if value is not None else {'unparsed_label': raw}
            except ValueError:
                return {'unparsed_label': raw}
        raw = self.read(payload + '/.landing_zones/landingzone-run-metadata.tsv')
        # Legacy labels have no content evidence: retain as provenance under relabel policy.
        return {'legacy_metadata_tsv': raw} if raw is not None else None

    def materialize(self, payload, target, accepted):
        if not self.remote:
            return self.root / payload
        target.mkdir(parents=True, exist_ok=True, mode=0o700)
        for key, item in accepted.items():
            path = target / key
            if item['kind'] == 'directory':
                path.mkdir(parents=True, exist_ok=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                self.transport.sftp.get(self.transport.path(payload + '/' + key), str(path))
        return target

    def remove(self, payload, accepted):
        if not self.remote:
            cleanup(self.root / payload, accepted)
            return
        t = self.transport
        if not t.exists(payload):
            return
        remaining = self.inspect(payload)
        if any(key not in accepted or item != accepted[key] for key, item in remaining.items()):
            raise ValueError('Source changed; cleanup refused')
        # Reinspect before removal; producer must relinquish writes after readiness.
        for key in sorted(remaining, key=lambda x: (x.count('/'), x), reverse=True):
            path = t.path(payload + '/' + key)
            if remaining[key]['kind'] == 'directory':
                t.sftp.rmdir(path)
            else:
                from .adapters import digest
                with t.sftp.open(path, 'rb') as handle:
                    if digest(handle) != accepted[key]['sha256']:
                        raise ValueError('Source changed during cleanup')
                t.sftp.remove(path)
        def remove_metadata(relative):
            attributes = t.check_path(t.path(relative))
            if stat.S_ISDIR(attributes.st_mode):
                for child in t.sftp.listdir_attr(t.path(relative)):
                    remove_metadata(relative + '/' + child.filename)
                t.sftp.rmdir(t.path(relative))
            elif stat.S_ISREG(attributes.st_mode):
                t.sftp.remove(t.path(relative))
            else:
                raise ValueError('Special metadata file refused')
        for key in (LABEL, '.ready', '.landing_zones'):
            if t.exists(payload + '/' + key):
                remove_metadata(payload + '/' + key)
        t.sftp.rmdir(t.path(payload))
