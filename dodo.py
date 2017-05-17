from __future__ import print_function
import os
import pandas
from doit.tools import run_once


def task_download():
    metadata = pandas.read_table('metadata.tsv.gz')
    for accession in sorted(metadata['Run_acc'].unique()):
        outfile = 'sra-fastq/{prefix}/{acc}.fq.gz'.format(prefix=accession[:5], acc=accession)
        cmd = 'fastq-dump --split-files -Z {acc} | gzip -c > {of}'.format(acc=accession, of=outfile)
        yield {
            'name': outfile,
            'file_dep': ['metadata.tsv.gz'],
            'actions': [cmd],
            'targets': [outfile],
            'uptodate': [run_once]
        }
