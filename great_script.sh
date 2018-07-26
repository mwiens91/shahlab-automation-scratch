#!/usr/bin/env bash

python query_gsc_for_wgs_bams.py '{"json_data": "results.json", "library_ids": ["A91716", "A91717"], "skip_file_import": true, "skip_older_than": "2015-07-24"}'
