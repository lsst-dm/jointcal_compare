#!/usr/bin/env python
"""
Generate slurm files for multiple validate jobs, parallelized across filters.
"""
from __future__ import print_function

import os
import sqlite3
import subprocess

import lsst.utils

base_slurm = """#!/bin/bash -l

#SBATCH -p normal
#SBATCH -N 1
#SBATCH --time=1440
#SBATCH -J {name}

source /software/lsstsw/stack/loadLSST.bash
setup validate_drp
setup obs_subaru
{setupOther}

pids=()

{cmd}

for pid in ${{pids[*]}};
do
    echo "Waiting: $pid"
    wait $pid  # Wait on all PIDs, this returns 0 if ANY process fails
done
"""
# NOTE: double-braces around {{pids}} is to prevent python .format() confusion.

# some useful globals
base_cmd = ("srun  --output=/project/parejkoj/DM-11783/logs/{name}_{filt}-%J.log"
            " matchedVisitMetrics.py {datadir} --output={output} -C={config}"
            " --id ccd={ccd} filter={filt} tract={tract} visit={visit}"
            " --longlog --no-versions &\n"
            "pids+=($!)  # Save PID of this background process")

root = '/project/parejkoj/DM-11783'
pkgdir = lsst.utils.getPackageDir('jointcal_compare')
ccd = "0..8^10..103"


def find_visits(cursor, tract, filt):
    """Return the visits that are in this tract."""
    cmd = "select distinct visit from calexp where tract=:tract and filter=:filt"
    cursor.execute(cmd, dict(tract=tract, filt=filt))
    result = cursor.fetchall()
    # NOTE: have to flatten the list, as it comes out as [(1,), (2,), (3,), ...]
    return '^'.join(str(x[0]) for x in result)


def generate_one(field, tract, filters, ccd, cursor, datadir, setupOther, call=True):
    """Generate and execute a slurm script."""

    output = os.path.join(outdir, str(tract))
    fmtstr = dict(output=output, field=field, tract=tract, ccd=ccd, setupOther=setupOther,
                  datadir=datadir, config=config, ntasks=len(filters))
    name = basename + "-{field}_{tract}".format(**fmtstr)
    fmtstr['name'] = name

    cmd_list = []
    for filt in filters:
        visit = find_visits(cursor, tract, filt)
        cmd_list.append(base_cmd.format(**fmtstr, filt=filt, visit=visit))
    cmd = '\n'.join(cmd_list)

    outlog = open(os.path.join(root, 'slurm-logs/{name}.log'.format(**fmtstr)), 'w')

    filename = os.path.join(root, 'scripts/{name}.sl'.format(**fmtstr))
    with open(filename, 'w') as outfile:
        outfile.write(base_slurm.format(cmd=cmd, **fmtstr))
    print('Generated:', filename)
    if call:
        cmd = 'sbatch %s'%filename
        subprocess.check_call(cmd.split(), stdout=outlog, stderr=subprocess.STDOUT)
        print('Launched job:', cmd)


def process_all(basename, datadir, outdir, config, setupOther, call=False):
    """Generate slurm scripts and optionally execute them."""
    # deep data
    tract = 9813
    field = "UDEEP"
    filters = ['HSC-Y', 'HSC-Z', 'HSC-I', 'HSC-R', 'HSC-G']
    conn = sqlite3.connect(os.path.join(sqlitedir, 'overlaps_SSPUDEEP_w15.sqlite3'))
    cursor = conn.cursor()
    generate_one(field, tract, filters, ccd, cursor, datadir.format(field=field), setupOther, call=call)

    # wide data
    tracts = [8521, 8522, 8523, 8524, 8525, 9558, 9559, 9560, 9561, 9371, 9372,
              9373, 9374, 9693, 9694, 9695, 9697, 9698, 15831, 15832, 16009, 16010]
    field = "WIDE"
    filters = ['HSC-Y', 'HSC-Z', 'HSC-I', 'HSC-R', 'HSC-G']
    conn = sqlite3.connect(os.path.join(sqlitedir, 'overlaps_SSPWIDE_w15.sqlite3'))
    cursor = conn.cursor()
    for tract in tracts:
        generate_one(field, tract, filters, ccd, cursor, datadir.format(field=field), setupOther, call=call)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("calSource", metavar="calSource",
                        choices=["jointcal", "single", "mosaic"],
                        help="Source of the calibration to run matchedVisitMetricsTask on")
    parser.add_argument("--jointcalRerun", default="DM-15617",
                        help="Rerun to read jointcal results from.")
    parser.add_argument("-c", "--call", action="store_true",
                        help="Call the generated slurm sbatch scripts to launch the jobs.")
    args = parser.parse_args()

    if args.calSource == 'jointcal':
        datadir = '/datasets/hsc/repo/rerun/private/parejkoj/'+args.jointcalRerun+'/{field}'
    else:
        datadir = '/datasets/hsc/repo/rerun/DM-13666/{field}'

    setupOther = 'setup meas_mosaic' if args.calSource == 'mosaic' else ''

    basename = 'validate-{}'.format(args.calSource)
    outdir = os.path.join(root, basename)
    config = os.path.join(pkgdir, 'config', basename+'Config.py')
    sqlitedir = os.path.join(root, 'tract-visit')

    process_all(basename, datadir, outdir, config, setupOther, call=args.call)
