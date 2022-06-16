#!/bin/bash
#SBATCH -N2

python run_bgr_flow_slurm.py \
hpc=true \
path_to_channel=/home/rpm/start_manual_test_services/GitHub/mas-infrastructure/src/cpp/common/_cmake_linux_release/channel \
path_to_monica=/home/rpm/start_manual_test_services/GitHub/monica/_cmake_linux_release/monica-capnp-fbp-component \
path_to_mas=home/rpm/start_manual_test_services/GitHub/mas-infrastructure \
path_to_klimertrag=/home/rpm/start_manual_test_services/GitHub/klimertrag \
path_to_out_dir=/home/rpm/start_manual_test_services/GitHub/klimertrag/out_fbp \
setups_file=/home/rpm/start_manual_test_services/GitHub/klimertrag/sim_setups_bgr_flow.csv \
coords_file=/beegfs/rpm/projects/monica/project/klimertrag/bgr/all_coord_shuffled_anonymous.csv

#echo "hello from" `/bin/hostname`
#srun -l /bin/hostname
#srun -l /bin/pwd