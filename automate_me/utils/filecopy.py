import os
import sys
import subprocess


def mkdir_with_mode(directory, mode):
    if not os.path.isdir(directory):
        oldmask = os.umask(000)
        os.makedirs(directory, mode)
        os.umask(oldmask)


def rsync_file(from_path, to_path):
    mkdir_with_mode(os.path.dirname(to_path), 0775)

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

