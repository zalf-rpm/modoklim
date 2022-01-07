from distutils.dir_util import copy_tree
import glob
import os

folder_lst = [
    "jaenicke_2879_2021-09-December_201657",
    "jaenicke_2880_2021-09-December_201812",
    "jaenicke_2881_2021-09-December_201908",
    "jaenicke_2882_2021-09-December_202017",
    "jaenicke_2883_2021-09-December_202049",
    "jaenicke_2884_2021-09-December_202124",
    "jaenicke_2885_2021-09-December_202159",
    "jaenicke_2886_2021-09-December_202232",
    "jaenicke_2887_2021-09-December_202306",
    "jaenicke_2888_2021-09-December_202337",
    "jaenicke_2889_2021-09-December_202403"
    ]

source_dir = "/beegfs/rpm/projects/monica/out"
target_dir = "/beegfs/jaenicke/klimertrag/raster/sim-yields/10_predictions_future"
for folder in folder_lst:
    from_dirs = glob.glob(f"{source_dir}/{folder}/out/*")
    for from_dir in from_dirs:
        base_name = os.path.basename(from_dir)
        to_dir = f"{target_dir}/{base_name}"
        print(from_dir, '\n', to_dir)
        copy_tree(from_dir, to_dir)
        print("Done!")
