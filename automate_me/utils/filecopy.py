import os
import sys
import subprocess

from utils import make_dirs


def rsync_file(from_path, to_path):
    make_dirs(os.path.dirname(to_path))

    subprocess_cmd = [
        'rsync',
        '--progress',
        '--chmod=D555',
        '--chmod=F444',
        '--times',
        '--copy-links',
        from_path,
        to_path,
    ]

    print ' '.join(subprocess_cmd)

    sys.stdout.flush()
    sys.stderr.flush()
    subprocess.check_call(subprocess_cmd, stdout=sys.stdout, stderr=sys.stderr)

    if os.path.getsize(to_path) != os.path.getsize(from_path):
        raise Exception('copy failed for {} to {}'.format(
            from_path, to_path))

