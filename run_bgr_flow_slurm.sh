#!/bin/bash
#SBATCH -N20 -c80

export MONICA_PARAMETERS=/home/rpm/start_manual_test_services/GitHub/monica-parameters

python run_bgr_flow_slurm.py \
hpc=true \
nodes=20 \
path_to_channel=/home/rpm/start_manual_test_services/GitHub/mas-infrastructure/src/cpp/common/_cmake_linux_release/channel \
path_to_monica=/home/rpm/start_manual_test_services/GitHub/monica/_cmake_linux_release/monica-capnp-fbp-component \
path_to_mas=/home/rpm/start_manual_test_services/GitHub/mas-infrastructure \
path_to_klimertrag=/home/rpm/start_manual_test_services/GitHub/klimertrag \
path_to_out_dir=/home/rpm/start_manual_test_services/GitHub/klimertrag/out_fbp \
path_to_dwd_csvs=/beegfs/common/data/climate/dwd/csvs \
setups_file=/home/rpm/start_manual_test_services/GitHub/klimertrag/sim_setups_bgr_flow.csv \
coords_file=/beegfs/rpm/projects/monica/project/klimertrag/bgr/all_coord_shuffled_anonymous.csv \
monica_count=30 \
proj_transformer_count=15 \
ilr_count=15 \
dwd_count=10 \
writer_count=5

#echo "hello from" `/bin/hostname`
#srun -l /bin/hostname
#srun -l /bin/pwd