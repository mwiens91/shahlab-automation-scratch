#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import logging
import os
import string
import sys
import time
import pandas as pd
from utils.constants import LOGGING_FORMAT
from utils.dlp import create_sequence_dataset_models, fastq_paired_end_check
from utils.filecopy import rsync_file
from utils.gsc import get_sequencing_instrument, GSCAPI
from utils.runtime_args import parse_runtime_args
from utils.colossus import ColossusApi
from utils.tantalus import TantalusApi

# Set up the root logger
logging.basicConfig(
    format=LOGGING_FORMAT,
    stream=sys.stdout,
    level=logging.INFO,
)

solexa_run_type_map = {
    'Paired': 'P'}


def reverse_complement(sequence):
    return str(sequence[::-1]).translate(string.maketrans('ACTGactg', 'TGACtgac'))


def decode_raw_index_sequence(raw_index_sequence, instrument, rev_comp_override):
    i7 = raw_index_sequence.split("-")[0]
    i5 = raw_index_sequence.split("-")[1]

    if rev_comp_override is not None:
        if rev_comp_override == 'i7,i5':
            pass
        elif rev_comp_override == 'i7,rev(i5)':
            i5 = reverse_complement(i5)
        elif rev_comp_override == 'rev(i7),i5':
            i7 = reverse_complement(i7)
        elif rev_comp_override == 'rev(i7),rev(i5)':
            i7 = reverse_complement(i7)
            i5 = reverse_complement(i5)
        else:
            raise Exception('unknown override {}'.format(rev_comp_override))

        return i7 + '-' + i5

    if instrument == 'HiSeqX':
        i7 = reverse_complement(i7)
        i5 = reverse_complement(i5)
    elif instrument == 'HiSeq2500':
        i7 = reverse_complement(i7)
    elif instrument == 'NextSeq550':
        i7 = reverse_complement(i7)
        i5 = reverse_complement(i5)
    else:
        raise Exception('unsupported sequencing instrument {}'.format(instrument))

    return i7 + '-' + i5


def query_colossus_dlp_cell_info(colossus_api, library_id):

    sublibraries = colossus_api.get_colossus_sublibraries_from_library_id(library_id)

    cell_samples = {}
    for sublib in sublibraries:
        index_sequence = sublib['primer_i7'] + '-' + sublib['primer_i5']
        cell_samples[index_sequence] = sublib['sample_id']['sample_id']

    return cell_samples


def query_colossus_dlp_rev_comp_override(colossus_api, library_id):
    library_info = colossus_api.query_libraries_by_library_id(library_id)

    rev_comp_override = {}
    for sequencing in library_info['dlpsequencing_set']:
        for lane in sequencing['dlplane_set']:
            rev_comp_override[lane['flow_cell_id']] = sequencing['dlpsequencingdetail']['rev_comp_override']

    return rev_comp_override


# Mapping from filename pattern to read end, pass/fail
filename_pattern_map = {
    '_1.fastq.gz': (1, True),
    '_1_*.concat_chastity_passed.fastq.gz': (1, True),
    '_1_chastity_passed.fastq.gz': (1, True),
    '_1_chastity_failed.fastq.gz': (1, False),
    '_1_*bp.concat.fastq.gz': (1, True),
    '_2.fastq.gz': (2, True),
    '_2_*.concat_chastity_passed.fastq.gz': (2, True),
    '_2_chastity_passed.fastq.gz': (2, True),
    '_2_chastity_failed.fastq.gz': (2, False),
    '_2_*bp.concat.fastq.gz': (2, True),
}


dlp_fastq_template = os.path.join(
    'single_cell_indexing',
    'fastq',
    '{primary_sample_id}',
    '{dlp_library_id}',
    '{flowcell_id}_{lane_number}',
    '{cell_sample_id}_{dlp_library_id}_{index_sequence}_{read_end}.fastq{extension}')


def import_gsc_dlp_paired_fastqs(colossus_api,tantalus_api, dlp_library_id, storage, existing_lanes, tag_name):
    primary_sample_id = colossus_api.query_libraries_by_library_id(dlp_library_id)['sample']['sample_id']
    cell_samples = query_colossus_dlp_cell_info(colossus_api, dlp_library_id)
    rev_comp_overrides = query_colossus_dlp_rev_comp_override(colossus_api, dlp_library_id)

    external_identifier = '{}_{}'.format(primary_sample_id, dlp_library_id)

    gsc_api = GSCAPI()

    library_infos = gsc_api.query('library?external_identifier={}'.format(external_identifier))

    if (library_infos) == 0:
        raise Exception('no libraries with external_identifier {} in gsc api'.format(external_identifier))
    elif len(library_infos) > 1:
        raise Exception('multiple libraries with external_identifier {} in gsc api'.format(external_identifier))

    library_info = library_infos[0]

    gsc_library_id = library_info['name']

    fastq_infos = gsc_api.query('fastq?parent_library={}'.format(gsc_library_id))

    fastq_file_info = []

    for fastq_info in fastq_infos:
        fastq_path = fastq_info['data_path']

        if fastq_info['status'] != 'production':
            logging.info('skipping file {} marked as {}'.format(
                fastq_info['data_path'], fastq_info['status']))
            continue

        flowcell_id = fastq_info['libcore']['run']['flowcell']['lims_flowcell_code']
        lane_number = fastq_info['libcore']['run']['lane_number']

        if (flowcell_id, str(lane_number)) in existing_lanes:
            logging.info('skipping file {} with existing lane {}_{}'.format(
                fastq_path, flowcell_id, lane_number))
            continue

        if fastq_info['removed_datetime'] is not None:
            logging.info('skipping file {} marked as removed {}'.format(
                fastq_info['data_path'], fastq_info['removed_datetime']))
            continue

        sequencing_instrument = get_sequencing_instrument(fastq_info['libcore']['run']['machine'])
        solexa_run_type = fastq_info['libcore']['run']['solexarun_type']
        read_type = solexa_run_type_map[solexa_run_type]

        primer_id = fastq_info['libcore']['primer_id']
        primer_info = gsc_api.query('primer/{}'.format(primer_id))
        raw_index_sequence = primer_info['adapter_index_sequence']

        logging.info('loading fastq %s, index %s, %s', fastq_info['id'], raw_index_sequence, fastq_path)

        flowcell_lane = flowcell_id
        if lane_number is not None:
            flowcell_lane = flowcell_lane + '_' + str(lane_number)

        rev_comp_override = rev_comp_overrides.get(flowcell_lane)

        index_sequence = decode_raw_index_sequence(raw_index_sequence, sequencing_instrument, rev_comp_override)

        filename_pattern = fastq_info['file_type']['filename_pattern']
        read_end, passed = filename_pattern_map.get(filename_pattern, (None, None))

        if read_end is None:
            raise Exception('Unrecognized file type: {}'.format(filename_pattern))

        if not passed:
            continue

        try:
            cell_sample_id = cell_samples[index_sequence]
        except KeyError:
            raise Exception('unable to find index {} for flowcell lane {} for library {}'.format(
                index_sequence, flowcell_lane, dlp_library_id))

        extension = ''
        compression = 'UNCOMPRESSED'
        if fastq_path.endswith('.gz'):
            extension = '.gz'
            compression = 'GZIP'
        elif not fastq_path.endswith('.fastq'):
            raise ValueError('unknown extension for filename {}'.format(fastq_path))

        tantalus_filename = dlp_fastq_template.format(
            primary_sample_id=primary_sample_id,
            dlp_library_id=dlp_library_id,
            flowcell_id=flowcell_id,
            lane_number=lane_number,
            cell_sample_id=cell_sample_id,
            index_sequence=index_sequence,
            read_end=read_end,
            extension=extension,
        )

        tantalus_path = os.path.join(
            storage['storage_directory'],
            tantalus_filename,
        )

        rsync_file(fastq_path, tantalus_path)

        fastq_file_info.append(dict(
            dataset_type='FQ',
            sample_id=cell_sample_id,
            library_id=dlp_library_id,
            library_type='SC_WGS',
            index_format='D',
            sequence_lanes=[dict(
                flowcell_id=flowcell_id,
                lane_number=lane_number,
                sequencing_centre='GSC',
                sequencing_instrument=sequencing_instrument,
                sequencing_library_id=gsc_library_id,
                read_type=read_type,
            )],
            size=os.path.getsize(fastq_path),
            created=pd.Timestamp(time.ctime(os.path.getmtime(fastq_path)), tz='Canada/Pacific'),
            file_type='FQ',
            read_end=read_end,
            index_sequence=index_sequence,
            compression=compression,
            filename=tantalus_filename,
        ))

    fastq_paired_end_check(fastq_file_info)

    create_sequence_dataset_models(fastq_file_info, storage['name'], tag_name, tantalus_api)


if __name__ == '__main__':
    # Parse the incoming arguments
    args = parse_runtime_args()

    # Connect to the Tantalus API (this requires appropriate environment
    colossus_api = ColossusApi()
    tantalus_api = TantalusApi()

    storage = tantalus_api.get('storage_server', name=args['storage_name'])

    # Get the tag name if it was passed in
    try:
        tag_name = args['tag_name']
    except KeyError:
        tag_name = None

    # Query tantalus for existing lanes
    existing_lanes = set()
    for lane in tantalus_api.list('sequencing_lane', dna_library__library_id=args['dlp_library_id']):
        existing_lanes.add((lane['flowcell_id'], lane['lane_number']))

    # Query GSC for FastQs
    import_gsc_dlp_paired_fastqs(
        colossus_api,
        tantalus_api,
        args['dlp_library_id'],
        storage,
        existing_lanes,
        tag_name)

    # Check if we skipped all files
    if not json_to_post:
        logging.error('no data to import')
        sys.exit()

    logging.info('import succeeded')
