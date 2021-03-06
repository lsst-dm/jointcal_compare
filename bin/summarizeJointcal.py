#!/usr/bin/env python
"""
Compare DM-15617 and DM-15713 jointcal runs (the second with higher order
polynomials than the first).

Ingest the ReStructured Text output from validate_drp's reportPerformance.py
and produce summary tables and plots across all tracts.

Plots are saved to the current working directory.

Run `reportPerformance.py` from this jointcal_compare/bin/ first, to generate
the necessary files.
"""
import glob
import os.path

import matplotlib
matplotlib.use('Agg')  # noqa: E402
import matplotlib.pyplot as plt

import numpy as np
import astropy.io.ascii
import astropy.table
import pandas as pd


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
    files = glob.glob(inglob.format(name))
    if files == []:
        raise RuntimeError("No files found for glob: %s"%inglob.format(name))
    for infile in files:
        tract = int(os.path.basename(infile).split('-')[0])
        temp = astropy.io.ascii.read(infile, format='rst',
                                     exclude_names=("Comments", "Release Target: FY17"),
                                     fill_values=[('--', '0'), ('**', '0')])
        temp.rename_column('SRD Requirement: design', 'Design')
        temp.rename_column('Value', 'Value_{}'.format(name))
        tables[tract] = temp
    return tables


def plotMetricScatter(data, df, name1, name2, band, descriptions, xmin=None, ymin=None):
    """
    Plot two metrics against each other, e.g. jointcal vs. mosaic linked by lines.

    Parameters
    ----------
    data : `astropy.table`
        Astropy table containing the merged data.
    df : `pandas.Dataframe`
        Dataframe containing the data.
    name1 : `str`
        Name of x-axis metric.
    name2 : `str`
        Name of y-axis metric.
    band : `str`
        Filter band to plot.
    descriptions : `dict` of `str`
        name: descriptions, used to label the plot axes.
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

    # draw lines from order5->order7
    value2 = 'Value_DM-15713'
    plt.scatter(t1['Value_DM-15617'], t2['Value_DM-15617'], label="low order", color="orange")
    plt.plot(np.concatenate([[s, j, None] for s, j in zip(t1['Value_DM-15617'],
                                                          t1[value2])]),
             np.concatenate([[s, j, None] for s, j in zip(t2['Value_DM-15617'],
                                                          t2[value2])]),
             'k', alpha=0.1, label="same tract")

    plt.scatter(t1['Value_DM-15713'], t2['Value_DM-15713'], label="high order", color="green")
    suffix = 'jointcal'

    plt.xlabel("%s: %s" % (name1, descriptions[name1]))
    plt.ylabel("%s: %s" % (name2, descriptions[name2]))
    if xmin is not None:
        plt.xlim(xmin=xmin)
    if ymin is not None:
        plt.ylim(ymin=ymin)
    plt.legend()
    filename = "%sv%s_%s-%s.png" % (name1, name2, band, suffix)
    plt.savefig(filename)
    plt.close()
    print("Wrote plot to:", filename)


def main(args):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", metavar="path", nargs='?', type=str, default='.',
                        help="Path containing the .rst files to process (default=%(default)s).")
    parser.add_argument("-p", "--plot", action="store_true",
                        help="Generate metric comparison plots.")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Be more verbose when reading and computing statistics.")
    parser.add_argument("-i", "--interactive", action="store_true",
                        help="Open an ipdb console before exiting.")
    args = parser.parse_args(args)

    inglob = os.path.join(args.path, "{}/performance/*-jointcal.rst")
    order5 = read_tables('DM-15617', inglob)
    order7 = read_tables('DM-15713', inglob)

    tracts = list(order5.keys())

    join_keys = ('Metric', 'Filter', 'Operator', 'Design')

    if args.verbose:
        print('counts per tract (should be identical): single, mosaic, jointcal')
    per_tract = {}
    for tract in tracts:
        # temp = astropy.table.join(single[tract], mosaic[tract], keys=join_keys, join_type='outer')
        temp = astropy.table.join(order5[tract], order7[tract], keys=join_keys, join_type='outer')
        if args.verbose:
            print(tract, len(order5[tract]), len(order7[tract]))
        temp['tract'] = tract  # for group_by()
        # These are identical, so we only need one of them.
        temp.remove_columns(['Unit_2'])
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

    if args.plot:
        for filt in filters:
            plotMetricScatter(data, df, "AM1", "AF1", filt, descriptions, xmin=0, ymin=0)
            plotMetricScatter(data, df, "AM2", "AF2", filt, descriptions, xmin=0, ymin=0)
            plotMetricScatter(data, df, "PA1", "PF1", filt, descriptions)

    def rms(x):
        """Compute the root mean squared of a distribution."""
        return np.sqrt(np.mean(x**2))

    def print_y_is_less(x, y, name, verbose=False):
        """Print a green `>` if y is less than x, otherwise a red `<`."""
        if (verbose or x < y):
            print(name, x, "\033[92m>\033[0m" if x > y else "\033[91m<\033[0m", y)

    # compute final summary statistics
    print("mosaic vs. jointcal metric RMSs")
    print("-------------------------------")
    for metric in ("AM1", "AF1", "AM2", "AF2", "PA1", "PF1"):
        print("7th order tracts that exceed the 5th order metric for", metric)
        test = (data['Metric'] == metric) & (data['tract'] != 9813)
        order5 = data[test]['Value_DM-15617']
        order7 = data[test]['Value_DM-15713']
        name = metric
        for x in data[test]:
            name = "{} {}".format(x['Filter'], x['tract'])
            print_y_is_less(x['Value_DM-15617'], x['Value_DM-15713'], name, verbose=args.verbose)
        print()

    if args.interactive:
        import ipdb
        ipdb.set_trace()


if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
