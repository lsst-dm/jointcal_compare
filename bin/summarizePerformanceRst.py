#!/usr/bin/env python
"""
Ingest the ReStructured Text output from validate_drp's reportPerformance.py
and produce summary tables and plots across all tracts.

Change the `root` global at the top to the directory containing the .rst files.
Plots are saved to the current working directory.

Run `reportPerformance.py` from this jointcal_compare/bin/ first, to generate
the necessary files.
"""
import glob
import os.path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import numpy as np
import astropy.io.ascii
import astropy.table
import pandas as pd

# The directory containing the files to process.
# root = "/project/parejkoj/DM-11783/performance"
# root = "/home/parejkoj/lsst/temp/sshfs-mount/DM-11783/performance"
root = "/Users/parejkoj/lsst/jointcal/temp/performance"
inglob = os.path.join(root, "*-{}.rst")


def read_tables(name, inglob):
    """Ingest the .rst files with astropy and return a dict of astropy.tables

    Parameters
    ----------
    name : `str`
        Metric name to read in.
    inglob : `str`
        glob pattern to use to search for files with (modified by `inglob.format(name)`)
    """
    tables = {}
    for infile in glob.glob(inglob.format(name)):
        tract = int(os.path.basename(infile).split('-')[0])
        temp = astropy.io.ascii.read(infile, format='rst',
                                     exclude_names=("Comments", "Release Target: FY17"),
                                     fill_values=[('--', '0'), ('**', '0')])
        temp.rename_column('SRD Requirement: design', 'Design')
        temp.rename_column('Value', 'Value_{}'.format(name))
        tables[tract] = temp
    return tables


def plotMetricScatter(df, name1, name2, band):
    """
    Plot two metrics against each other, jointcal vs. meas_mosaic linked by lines.

    Parameters
    ----------
    df : `pandas.Dataframe`
        Dataframe containing the data.
    name1 : `str`
        Name of x-axis metric.
    name2 : `str`
        Name of y-axis metric.
    band : `str`
        Filter band to plot.
    """

    limit1 = data[data['Metric'] == name1]['Design'][0]
    limit2 = data[data['Metric'] == name2]['Design'][0]

    t1 = df.loc[name1].loc[band]
    t2 = df.loc[name2].loc[band]
    title = "%s vs. %s: %s" % (name1, name2, band)
    plt.figure(title, figsize=(8, 6))
    assert np.all(t1.tract == t2.tract)
    plt.title(title)
    plt.axvline(limit1, color='grey', ls='--')
    plt.axhline(limit2, color='grey', ls='--')
    plt.scatter(t1.Value_singleFrame, t2.Value_singleFrame, label="singleFrame")
    plt.scatter(t1.Value_meas_mosaic, t2.Value_meas_mosaic, label="meas_mosaic")
    plt.scatter(t1.Value_jointcal, t2.Value_jointcal, label="jointcal")

    # draw lines from singleFrame->meas_mosaic and singleFrame->jointcal
    plt.plot(np.concatenate([[s, j, None] for s, j in zip(t1.Value_singleFrame,
                                                          t1.Value_meas_mosaic)]),
            np.concatenate([[s, j, None] for s, j in zip(t2.Value_singleFrame,
                                                         t2.Value_meas_mosaic)]),
            'k', alpha=0.1, label="same tract")
    plt.plot(np.concatenate([[s, j, None] for s, j in zip(t1.Value_singleFrame,
                                                          t1.Value_jointcal)]),
            np.concatenate([[s, j, None] for s, j in zip(t2.Value_singleFrame,
                                                         t2.Value_jointcal)]),
            'k', alpha=0.1, label="same tract")

    plt.xlabel("%s: %s" % (name1, descriptions[name1]))
    plt.ylabel("%s: %s" % (name2, descriptions[name2]))
    plt.legend()
    filename = "%sv%s_%s.png" % (name1, name2, band)
    plt.savefig(filename)
    print("Write plot to:", filename)


values = ('singleFrame', 'meas_mosaic', 'jointcal')

jointcal = read_tables('jointcal', inglob)
meas_mosaic = read_tables('meas_mosaic', inglob)
singleFrame = read_tables('singleFrame', inglob)

tracts = list(singleFrame.keys())

join_keys = ('Metric', 'Filter', 'Operator', 'Design')

print('counts per tract (should be identical): singleFrame, meas_mosaic, jointcal')
per_tract = {}
for tract in tracts:
    temp = astropy.table.join(singleFrame[tract], meas_mosaic[tract], keys=join_keys, join_type='outer')
    temp = astropy.table.join(temp, jointcal[tract], keys=join_keys, join_type='outer')
    print(tract, len(singleFrame[tract]), len(meas_mosaic[tract]), len(jointcal[tract]))
    temp['tract'] = tract  # for group_by()
    # The singleFrame Unit column (Unit_1) is going to have values for all fields,
    # others may not (i.e. if it wasn't measured).
    temp.remove_columns(['Unit', 'Unit_2'])
    temp.rename_column('Unit_1', 'Unit')
    per_tract[tract] = temp

data = astropy.table.vstack(list(per_tract.values()))

filters = set(data['Filter'])

df = data.to_pandas()
df.set_index(pd.MultiIndex.from_arrays([df.Metric, df.Filter]), inplace=True)

descriptions = {
    "AM1": "repeatability (marcsec) for pairs at 5 arcmin",
    "AM2": "repeatability (marcsec) for pairs at 20 arcmin",
    "AF1": "outlier fraction (%) for pairs at 5 min",
    "AF2": "outlier fraction (%) for pairs at 20 min",
    "PA1": "repeatability (mmag) of PSF source magnitudes",
    "PF1": "outlier fraction (%) deviating by more than PA2"
}

for filt in filters:
    plotMetricScatter(df, "AM1", "AF1", filt)
    plotMetricScatter(df, "AM2", "AF2", filt)
    plotMetricScatter(df, "PA1", "PF1", filt)

print()
print("jointcal calibrations that exceed metrics for a given filter+tract")
print("------------------------------------------------------------------")
for metric in ("AM1", "AF1", "AM2", "PA1"):
    print("Metric:", metric)
    print("-----------")
    test = data['Metric'] == metric
    limit = data[test]['Design']
    exceed = data[test]['Value_jointcal'] >= limit
    for x in data[test][exceed]:
        print("{} > {} for: {} {}".format(x['Value_jointcal'], x['Design'], x['Filter'], x['tract']))
    print()

# uncomment this to muck around with the metrics and look at on-screen plots.
#import ipdb; ipdb.set_trace()
