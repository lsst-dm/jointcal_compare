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

#SBATCH -p debug
#SBATCH -N 1
#SBATCH --time=1440
#SBATCH -J {name}

source /software/lsstsw/stack/loadLSST.bash
setup -r /project/parejkoj/stack/validate_drp/
setup -jkr /project/parejkoj/stack/validate_base
setup -jkr /project/parejkoj/stack/afw
setup -jkr /project/parejkoj/stack/obs_subaru
setup -jkr /project/parejkoj/stack/daf_persistence

{cmd}

for pid in ${{pids[*]}};
do
    wait $pid #Wait on all PIDs, this returns 0 if ANY process fails
done
"""
# NOTE: double-braces around {{pids}} is to prevent python .format() confusion.

base_cmd = ("srun  --output=/project/parejkoj/DM-11783/logs/{name}_{filt}-%J.log"
            " matchedVisitMetrics.py {datadir} --output={output} -C={config}"
            " --id ccd={ccd} filter={filt} tract={tract} field={field} visit={visit}"
            " --longlog --no-versions &\n"
            "pids[$PROC]=$!    #Save PID of this background process")

basename = 'validate-jointcal'

pkgdir = lsst.utils.getPackageDir('jointcal_compare')

sqlitedir = '/project/parejkoj/DM-11783/tract-visit'
datadir = '/datasets/hsc/repo/rerun/DM-10404/SFM'
outdir = os.path.join('/project/parejkoj/DM-11783', basename)
config = os.path.join(pkgdir, 'config', basename+'Config.py')

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
    name = basename + "-{field}_{tract}".format(**fmtstr)
    fmtstr['name'] = name

    cmd_list = []
    for filt in filters:
        visit = find_visits(cursor, tract, filt, field)
        cmd_list.append(base_cmd.format(**fmtstr, filt=filt, visit=visit))
    cmd = '\n'.join(cmd_list)

    outlog = open('/project/parejkoj/DM-11783/slurm-logs/{name}.log'.format(**fmtstr), 'w')

    filename = '/project/parejkoj/DM-11783/scripts/{name}.sl'.format(**fmtstr)
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
