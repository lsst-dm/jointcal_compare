#!/usr/bin/env python
"""
Generate slurm files for multiple validate jobs.`
"""
from __future__ import print_function

import os
import sqlite3
import subprocess

#SBATCH --output=/scratch/parejkoj/compare/logs/{name}-%j.log
#SBATCH --error=/scratch/parejkoj/compare/logs/{name}-%j.err
#SBATCH --ntasks={ntasks}

base_slurm = """#!/bin/bash -l

#SBATCH -p debug
#SBATCH -N 1
#SBATCH --time=1440
#SBATCH -J {name}

source /software/lsstsw/stack/loadLSST.bash
setup -r /scratch/parejkoj/validate_drp/
setup -r /scratch/parejkoj/meas_mosaic
setup -jkr /scratch/parejkoj/validate_base
setup -jkr /scratch/parejkoj/treecorr/_eupspkg/binary/treecorr/3.2.3.lsst2-1-g2722dd9/
setup -jkr /scratch/parejkoj/afw
setup -jkr /scratch/parejkoj/obs_subaru
setup -jkr /scratch/parejkoj/daf_persistence

{cmd}
"""

base_cmd = ("srun  --output=/scratch/parejkoj/compare/logs/{name}_{filt}.log"
            " matchedVisitMetrics.py {datadir} --output={output} -C={config}"
            " --id ccd={ccd} filter={filt} tract={tract} field={field} visit={visit}"
            " --longlog --no-versions")

sqlitedir = '/scratch/hchiang2/parejko/'
datadir = '/datasets/hsc/repo/rerun/private/lauren/DM-11786'
outdir = 'validate-meas_mosaic'
config = 'validateConfig-meas_mosaic.py'
# rerun = 'DM-10404/SFM:private/lauren/DM-11785/'

ccd = "0..8^10..103"

call = True


def find_visits(cursor, tract, filt, field):
    cmd = "select distinct visit from calexp where tract=:tract and filter=:filt"
    cursor.execute(cmd, dict(tract=tract, filt=filt))
    result = cursor.fetchall()
    # NOTE: have to flatten the list, as it comes out as [(1,), (2,), (3,), ...]
    return '^'.join(str(x[0]) for x in result)


def generate_one(field, tract, filters, ccd, cursor, call=True):
    """Generate and execute a slurm script."""

    output = os.path.join(outdir, str(tract))
    fmtstr = dict(output=output, field=field, tract=tract, ccd=ccd,
                  datadir=datadir, config=config, ntasks=len(filters))
    name = "validate-{field}_{tract}".format(**fmtstr)
    fmtstr['name'] = name

    cmd_list = []
    for filt in filters:
        visit = find_visits(cursor, tract, filt, field)
        cmd_list.append(base_cmd.format(**fmtstr, filt=filt, visit=visit))
    cmd = ' &\n'.join(cmd_list)

    outlog = open('/scratch/parejkoj/compare/slurm-logs/{name}.log'.format(**fmtstr), 'w')

    filename = 'scripts/{name}.sl'.format(**fmtstr)
    with open(filename, 'w') as outfile:
        outfile.write(base_slurm.format(cmd=cmd, **fmtstr))
    print('Generated:', filename)
    if call:
        cmd = 'sbatch %s'%filename
        subprocess.check_call(cmd.split(), stdout=outlog, stderr=subprocess.STDOUT)
        print('Launched job:', cmd)


# deep data:
tract = 9813
field = "SSP_UDEEP_COSMOS"
filters = ['HSC-Y', 'HSC-Z', 'HSC-I', 'HSC-R', 'HSC-G']
conn = sqlite3.connect(os.path.join(sqlitedir, 'dbUDEEP.sqlite3'))
cursor = conn.cursor()
generate_one(field, tract, filters, ccd, cursor, call=call)

# wide data:
tracts = [8521, 8522, 8523, 8524, 8525, 9558, 9559, 9560, 9561, 9371, 9372,
          9373, 9374, 9693, 9694, 9695, 9697, 9698, 15831, 15832, 16009, 16010]
field = "SSP_WIDE"
filters = ['HSC-Y', 'HSC-Z', 'HSC-I', 'HSC-R', 'HSC-G']
conn = sqlite3.connect(os.path.join(sqlitedir, 'dbWIDE.sqlite3'))
cursor = conn.cursor()
for tract in tracts:
    generate_one(field, tract, filters, ccd, cursor, call=call)
