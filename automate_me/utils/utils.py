from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import errno
import hashlib
import os


def get_lane_str(lane):
    if lane['lane_number'] == '':
        return '{}'.format(lane['flowcell_id'])
    else:
        return '{}_{}'.format(lane['flowcell_id'], lane['lane_number'])


def get_lanes_str(lanes):
    if len(lanes) == 0:
        raise ValueError('bam with no lanes')

    if len(lanes) == 1:
        return get_lane_str(lanes[0])

    else:
        lanes = ', '.join(sorted([get_lane_str(a) for a in lanes]))
        lanes = hashlib.md5(lanes)
        return '{}'.format(lanes.hexdigest()[:8])


def make_dirs(dirname, mode=0775):
    oldmask = os.umask(000)
    try:
        os.makedirs(dirname, mode)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(dirname):
            raise
    finally:
        os.umask(oldmask)

