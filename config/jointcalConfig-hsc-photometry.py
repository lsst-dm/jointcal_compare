config.doAstrometry = False

config.doPhotometry = True
config.photometryModel = "constrainedMagnitude"
config.photometryVisitOrder = 7

config.astrometryRefObjLoader.ref_dataset_name='ps1_pv3_3pi_20170110'
config.photometryRefObjLoader.ref_dataset_name='ps1_pv3_3pi_20170110'
config.astrometryRefObjLoader.filterMap={'B': 'g', 'r2': 'r', 'N1010': 'z', 'N816': 'i', 'I': 'i', 'N387': 'g', 'i2': 'i', 'R': 'r', 'N921': 'z', 'N515': 'g', 'V': 'r'}
config.photometryRefObjLoader.filterMap={'B': 'g', 'r2': 'r', 'N1010': 'z', 'N816': 'i', 'I': 'i', 'N387': 'g', 'i2': 'i', 'R': 'r', 'N921': 'z', 'N515': 'g', 'V': 'r'}
