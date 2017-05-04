#!/usr/bin/env python

from __future__ import print_function
import argparse
import pandas
import subprocess
import sys


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', '--ksize', metavar='K', type=int, default=27,
                        help='word size')
    parser.add_argument('--metadata', metavar='FILE',
                        default='seq_file_mapping_to_SRA.txt',
                        help='file from which metadata will be read; default '
                        'is "seq_file_mapping_to_SRA.txt"')
    parser.add_argument('--dry-run', action='store_true', help='show the '
                        'commands to be executed but do not run the commands!')
    parser.add_argument('sample', nargs='+', help='sample accession(s)')
    return parser


def get_runs_by_sample(infile):
    run_seen = set()
    metadata = pandas.read_table(infile)
    for run, sample in zip(metadata['Run_acc'], metadata['Sample_Acc']):
        if run not in run_seen:
            run_seen.add(run)
            yield run, sample


def collect_runs(infile, sample_accessions):
    run_index = dict()
    for sample in sample_accessions:
        run_index[sample] = set()

    for run, sample in get_runs_by_sample(infile):
        if sample in run_index:
            run_index[sample].add(run)

    for sample in sorted(list(run_index.keys())):
        runs = run_index[sample]
        yield sample, sorted(list(runs))


def build_stream(runlist, outfile, ksize=27, dryrun=False):
    cmd1 = ['fastq-dump', '--split-files', '-Z'] + runlist
    cmd2 = ['sourmash', 'compute', '-o', outfile, '--ksizes', str(ksize), '-']
    cmd1str = ' '.join(cmd1)
    cmd2str = ' '.join(cmd2)
    if dryrun:
        return None, cmd1str, cmd2str

    dumpprocess = subprocess.Popen(cmd1, stdout=subprocess.PIPE)
    minhashprocess = subprocess.Popen(cmd2, stdin=dumpprocess.stdout)
    return minhashprocess, cmd1str, cmd2str


def main(args):
    for sample, runlist in collect_runs(args.metadata, args.sample):
        outfile = '{:s}.minhash'.format(sample)
        process, cmd1, cmd2 = build_stream(runlist, outfile, args.ksize,
                                           args.dry_run)
        print('[sourrice]', cmd1, '|', cmd2, file=sys.stderr)
        if process:
            print('[sourrice] computing', outfile, file=sys.stderr)
            process.communicate()


if __name__ == '__main__':
    main(get_parser().parse_args())
