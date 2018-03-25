#!/bin/bash
# Summarize validate_drp performance per-tract.

function do_one_tract()
{
  if [ -d "$1" ]; then
    TRACT=$(basename $1)
    OUTPATH=$OUTDIR/$TRACT-$TASK.rst
    reportPerformance.py --output_file=$OUTPATH "$1"/*.json &
    pids+=($!)  # Save PID of this background process
    echo "Processing $TRACT to write $OUTPATH with pid: ${pids[-1]}"
  else
    echo "-" "$1" "is not a directory."
  fi
}

if [[ $* =~ ^(jointcal|singleFrame|meas_mosaic)$ ]]; then
  TASK=$*
else
  echo 'Please specify one of "jointcal", "singleFrame", "meas_mosaic" as the first argument.'
  exit -1
fi

source /software/lsstsw/stack/loadLSST.bash
setup validate_drp

ROOT=/project/parejkoj/DM-11783
VALIDATE=$ROOT/validate-$TASK
OUTDIR=$ROOT/performance

pids=()

for dir in $VALIDATE/*; do
    do_one_tract "$dir"
done

for pid in ${pids[*]};
do
    echo "Waiting: $pid"
    wait $pid  # Wait on all PIDs, this returns 0 if ANY process fails
done
