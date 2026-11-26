"""Keep inherited transfer locks open while a child that closes extra FDs is running."""
import signal
import subprocess
import sys


if __name__ == '__main__':
    # The worker may be interrupted before the remote command has returned.
    # Default signal actions are restored in the exec'd child. This process
    # waits for that child even when the foreground process group receives TERM.
    def wait_for_child(signum, frame):
        pass

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, wait_for_child)
    code = subprocess.call(sys.argv[1:])
    sys.exit(code if code >= 0 else 128 - code)
