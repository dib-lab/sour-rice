import os
import pandas
from doit.tools import run_once
try:
    from urllib.request import urlretrieve
except:
    from urllib import urltrerieve


def task_get_metadata():
    def get_metadata(targets):
        url = ('ftp://climb.genomics.cn/pub/10.5524/200001_201000/200001/'
               'seq_file_mapping_to_SRA.txt')
        urlretrieve(url, targets[0])
        return True

    return {
        'actions': [get_metadata],
        'targets': ['metadata.tsv'],
        'uptodate': [run_once],
    }


def task_get_samples():
    def get_samples(targets):
        metadata = pandas.read_table('metadata.tsv')
        with open('sample-accessions.txt', 'w') as outfile:
            for accession in metadata['Sample_Acc'].unique():
                print(accession, file=outfile)

    return {
        'actions': [get_samples],
        'targets': ['sample-accessions.txt'],
        'file_dep': ['metadata.tsv'],
    }


def task_run_sourrice():
    if not os.path.exists('sample-accessions.txt'):
        return False

    with open('sample-accessions.txt', 'r') as infile:
        for line in infile:
            accession = line.strip()
            cmd = './sourrice.py -k 27 ' + accession
            outfile = accession + '.minhash'
            yield {
                'name': outfile,
                'actions': [cmd],
                'targets': [outfile],
                'uptodate': [run_once],
            }
