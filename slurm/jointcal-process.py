#!/usr/bin/env python
"""
Generate slurm files for multiple jointcal jobs.`
"""
from __future__ import print_function

import os
import sqlite3
import subprocess

base_slurm = """#!/bin/bash -l

#SBATCH -p debug
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH --time=1440
#SBATCH -J {name}
#SBATCH --output=logs/{name}-%j.log
#SBATCH --error=logs/{name}-%j.err

source /software/lsstsw/stack/loadLSST.bash
setup -r /scratch/parejkoj/jointcal/
setup -jkr /scratch/parejkoj/obs_subaru
setup -jkr /scratch/parejkoj/daf_persistence

srun jointcal.py {datadir} --rerun={rerun} -C={config} --id ccd={ccd} filter={filt} tract={tract} field={field} visit={visit} --longlog --no-versions
"""

sqlitedir = '/scratch/hchiang2/parejko/'
datadir = '/datasets/hsc/repo'
# config = 'jointcalConfig-hsc-astrometry.py'
config = 'jointcalConfig-hsc.py'
rerun = 'DM-10404/SFM:private/parejkoj/DM-11785'

ccd = "0..8^10..103"


def find_visits(cursor, tract, filt, field):
    cmd = "select distinct visit from calexp where tract=:tract and filter=:filt"
    cursor.execute(cmd, dict(tract=tract, filt=filt))
    result = cursor.fetchall()
    # NOTE: have to flatten the list, as it comes out as [(1,), (2,), (3,), ...]
    return '^'.join(str(x[0]) for x in result)


def generate_one(output, field, tract, filt, ccd, cursor, call=True):
    """Generate and execute a slurm script."""

    visit = find_visits(cursor, tract, filt, field)

    fmtstr = dict(field=field, tract=tract, filt=filt, ccd=ccd, visit=visit,
                  datadir=datadir, rerun=rerun, config=config)
    name = "jointcal-{field}_{tract}_{filt}".format(**fmtstr)
    fmtstr['name'] = name

    outlog = open('slurm-logs/{name}.log'.format(**fmtstr), 'w')

    filename = 'scripts/{name}.sl'.format(**fmtstr)
    with open(filename, 'w') as outfile:
        outfile.write(base_slurm.format(**fmtstr))
    print('Generated:', filename)
    if call:
        cmd = 'sbatch %s'%filename
        subprocess.check_call(cmd.split(), stdout=outlog, stderr=subprocess.STDOUT)
        print('Launched job:', cmd)


# deep data:
output = 'jointcal-hsc-deep'
tract = 9813
field = "SSP_UDEEP_COSMOS"
filters = ['HSC-Y', 'HSC-Z', 'HSC-I', 'HSC-R', 'HSC-G', 'NB0921']
conn = sqlite3.connect(os.path.join(sqlitedir, 'dbUDEEP.sqlite3'))
cursor = conn.cursor()
for filt in filters:
    generate_one(output, field, tract, filt, ccd, cursor, call=True)

# wide data:
output = 'jointcal-hsc-wide'
tracts = [8521, 8522, 8523, 8524, 8525, 9558, 9559, 9560, 9561, 9371, 9372, 9373, 9374, 9693, 9694, 9695, 9697, 9698, 15831, 15832, 16009, 16010]
field = "SSP_WIDE"
filters = ['HSC-Y', 'HSC-Z', 'HSC-I', 'HSC-R', 'HSC-G']
conn = sqlite3.connect(os.path.join(sqlitedir, 'dbWIDE.sqlite3'))
cursor = conn.cursor()
for tract in tracts:
    for filt in filters:
        generate_one(output, field, tract, filt, ccd, cursor, call=True)

