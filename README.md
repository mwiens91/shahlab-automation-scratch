[![https://www.singularity-hub.org/static/img/hosted-singularity--hub-%23e32929.svg](https://www.singularity-hub.org/static/img/hosted-singularity--hub-%23e32929.svg)](https://singularity-hub.org/collections/1334)
![Python version](https://img.shields.io/badge/python-2-blue.svg)

# shahlab-automation-scratch

The only purpose of this repository is to have [Singularity
Hub](https://www.singularity-hub.org/) robots detect a [Singularity
container](https://www.sylabs.io/) build recipe in this repository and
build it on Singularity Hub.

The code in here is ripped from
[Tantalus](https://github.com/shahcompbio/tantalus) and is written by a
bunch of different people. See Tantalus' repo if you care about
authorship.

## Setup

Make sure the shell you're running this container on has the following
environment variables defined:

+ TANTALUS_API_USERNAME
+ TANTALUS_API_PASSWORD
+ GSC_API_USERNAME
+ GSC_API_PASSWORD

Additionally, the (dlp_bam_import.py)[automate_me/dlp_bam_import.py]
script needs the following environment variables defined:

+ AZURE_STORAGE_ACCOUNT
+ AZURE_STORAGE_KEY

The variable names should be self-explanatory.
