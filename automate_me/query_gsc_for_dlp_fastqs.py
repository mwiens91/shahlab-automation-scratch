#!/usr/bin/env python

import json
import os
import string
import sys
import time
import pandas as pd
from utils.colossus import (
    get_colossus_sublibraries_from_library_id,
    query_libraries_by_library_id,)
from utils.dlp import create_sequence_dataset_models, fastq_paired_end_check
from utils.gsc import get_sequencing_instrument, GSCAPI
from utils.tantalus import TantalusApi


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


def query_colossus_dlp_cell_info(library_id):

    sublibraries = get_colossus_sublibraries_from_library_id(library_id)

    cell_samples = {}
    for sublib in sublibraries:
        index_sequence = sublib['primer_i7'] + '-' + sublib['primer_i5']
        cell_samples[index_sequence] = sublib['sample_id']['sample_id']

    return cell_samples


def query_colossus_dlp_rev_comp_override(library_id):
    library_info = query_libraries_by_library_id(library_id)

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


def query_gsc_dlp_paired_fastqs(dlp_library_id, gsc_library_id):
    #storage = dict(name='gsc')

    cell_samples = query_colossus_dlp_cell_info(dlp_library_id)
    rev_comp_overrides = query_colossus_dlp_rev_comp_override(dlp_library_id)

    gsc_api = GSCAPI()

    fastq_infos = gsc_api.query('fastq?parent_library={}'.format(gsc_library_id))

    fastq_file_info = []

    for fastq_info in fastq_infos:
        if fastq_info['status'] != 'production':
            continue

        if fastq_info['removed_datetime'] is not None:
            continue

        fastq_path = fastq_info['data_path']
        flowcell_id = fastq_info['libcore']['run']['flowcell']['lims_flowcell_code']
        lane_number = fastq_info['libcore']['run']['lane_number']
        sequencing_instrument = get_sequencing_instrument(fastq_info['libcore']['run']['machine'])
        solexa_run_type = fastq_info['libcore']['run']['solexarun_type']
        read_type = solexa_run_type_map[solexa_run_type]

        primer_id = fastq_info['libcore']['primer_id']
        primer_info = gsc_api.query('primer/{}'.format(primer_id))
        raw_index_sequence = primer_info['adapter_index_sequence']

        print 'loading fastq', fastq_info['id'], 'index', raw_index_sequence, fastq_path

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

        # ASSUMPTION: GSC stored files are pathed from root
        fastq_filename_override = fastq_path

        # ASSUMPTION: meaningful path starts at library_name
        fastq_filename = fastq_path[fastq_path.find(gsc_library_id):]
        # Prepend single_cell_indexing/<gsc_lib_id>/ so it follows our file structure
        fastq_filename = 'single_cell_indexing/HiSeq/' + fastq_filename

        try:
            cell_sample_id = cell_samples[index_sequence]
        except KeyError:
            raise Exception('unable to find index {} for flowcell lane {} for library {}'.format(
                index_sequence, flowcell_lane, dlp_library_id))

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
                read_type=read_type,
            )],
            size=os.path.getsize(fastq_path),
            created=pd.Timestamp(time.ctime(os.path.getmtime(fastq_path)), tz='Canada/Pacific'),
            file_type='FQ',
            read_end=read_end,
            index_sequence=index_sequence,
            compression='GZIP',
            filename=fastq_filename,
            filename_override=fastq_filename_override,
        ))

    storage_name = 'gsc'

    fastq_paired_end_check(fastq_file_info)

    json_list = create_sequence_dataset_models(fastq_file_info, storage_name)

    return json_list


if __name__ == '__main__':
    # Parse the incoming arguments
    args = json.loads(sys.argv[1])

    # Connect to the Tantalus API (this requires appropriate environment
    # variables defined)
    tantalus_api = TantalusApi()

    # Query GSC for FastQs
    json_to_post = query_gsc_dlp_paired_fastqs(
        args['dlp_library_id'],
        args['gsc_library_id'],)

    # Get the tag name if it was passed in
    try:
        tag_name = args['tag_name']
    except KeyError:
        tag_name = None

    # Post data to Tantalus
    tantalus_api.sequence_dataset_add(
        model_dictionaries=json_to_post,
        tag_name=tag_name)
