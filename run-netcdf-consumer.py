#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

#print sys.path

from collections import defaultdict#, OrderedDict
import csv
#from datetime import datetime
from netCDF4 import Dataset
#import types
#import json
#import gc
import numpy as np
import os
#import sqlite3
import sys
import time
#import timeit
import zmq

import monica_io3
#import soil_io3
#print "path to monica_io: ", monica_io.__file__
import monica_run_lib as Mrunlib

PATHS = {
    "mbm-local-remote": {
        "path-to-data-dir": "monica-data/data/",
        "path-to-output-dir": "out/",
        "path-to-csv-output-dir": "csv-out/"
    },
    "remoteConsumer-remoteMonica": {
        "path-to-data-dir": "./monica-data/data/",
        "path-to-output-dir": "/out/out/",
        "path-to-csv-output-dir": "/out/csv-out/"
    }
}
DEFAULT_HOST = "login01.cluster.zalf.de" # "localhost" 
DEFAULT_PORT = "7777"
TEMPLATE_SOIL_PATH = "{local_path_to_data_dir}germany/BUEK200_1000_gk5.asc"
TEMPLATE_CORINE_PATH = "{local_path_to_data_dir}germany/landuse_1000_gk5.asc"
#TEMPLATE_SOIL_PATH = "{local_path_to_data_dir}germany/BUEK250_1000_gk5.asc"
#DATA_SOIL_DB = "germany/buek200.sqlite"
USE_CORINE = False

STAGES = [
    "Sowing", "Stage-2", "cereal-stem-elongation", "Stage-3", "Stage-4",
    "Stage-5", "Stage-6", "Stage-7", "Harvest"
]

def write_output_to_netcdfs(row, col, msg_data, ncs, is_bgr):

    for data in msg_data:
        results = data.get("results", [])

        if is_bgr:
            nc = ncs[0]
            shape = nc["sm"].shape 
            if shape[0] == 0:
                nc["sm"][:,:,:] = np.full((len(results), shape[1], shape[2]), -99)
                nc["st"][:,:,:] = np.full((len(results), shape[1], shape[2]), -99)

            for i in range(0, 20):
                nc = ncs[i]
                arr = np.fromiter(map(lambda v: v[i], results[0]), dtype="i1") #Mois
                nc["sm"][row, col, :] = arr
                arr = np.fromiter(map(lambda v: v[i], results[1]), dtype="i1") #Stemp
                nc["st"][row, col, :] = arr
                print(i, end=" ", flush=True)

        else:
            if len(results) == 0:
                continue
            section = data.get("origSpec", "xx")[1:-1]
            nc = ncs
            shape = nc["Sowing_doy"].shape
            if shape[0] == 0:
                for name, var in nc.variables.items():
                    n = name.split("_")[1]
                    if n == "sm":
                        var[:,:,:] = np.full((len(results), shape[1], shape[2]), -99)
                    elif n == "doy":
                        var[:,:,:] = np.full((len(results), shape[1], shape[2]), -99)

            nc[section + "_doy"][:, row, col] = results[0]
            nc[section + "_sm_0-30"][:, row, col] = results[1]
            nc[section + "_sm_30-60"][:, row, col] = results[2]
            nc[section + "_sm_60-90"][:, row, col] = results[3]


def run_consumer(leave_after_finished_run = True, server = {"server": None, "port": None}, shared_id = None):
    "collect data from workers"

    config = {
        "mode": "mbm-local-remote",
        "port": server["port"] if server["port"] else DEFAULT_PORT,
        "server": server["server"] if server["server"] else DEFAULT_HOST, 
        "start-row": "0",
        "end-row": "-1",
        "shared_id": shared_id,
        "no-of-setups": 10,
        "scratch": "scratch/", #"/scratch/rpm/",
        "timeout": 600000 # 10 minutes
    }

    if len(sys.argv) > 1 and __name__ == "__main__":
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v

    paths = PATHS[config["mode"]]

    if not "out" in config:
        config["out"] = paths["path-to-output-dir"]
    if not "csv-out" in config:
        config["csv-out"] = paths["path-to-csv-output-dir"]

    print("consumer config:", config)

    context = zmq.Context()
    if config["shared_id"]:
        socket = context.socket(zmq.DEALER)
        socket.setsockopt(zmq.IDENTITY, config["shared_id"])
    else:
        socket = context.socket(zmq.PULL)

    socket.connect("tcp://" + config["server"] + ":" + config["port"])
    socket.RCVTIMEO = config["timeout"]
    leave = False
    write_normal_output_files = False

    path_to_soil_grid = TEMPLATE_SOIL_PATH.format(local_path_to_data_dir=paths["path-to-data-dir"])
    soil_metadata, header = Mrunlib.read_header(path_to_soil_grid)
    soil_grid_template = np.loadtxt(path_to_soil_grid, dtype=int, skiprows=6)

    if USE_CORINE:
        path_to_corine_grid = TEMPLATE_CORINE_PATH.format(local_path_to_data_dir=paths["path-to-data-dir"])
        corine_meta, _ = Mrunlib.read_header(path_to_corine_grid)
        corine_grid = np.loadtxt(path_to_corine_grid, dtype=int, skiprows=6)
        corine_gk5_interpolate = Mrunlib.create_ascii_grid_interpolator(corine_grid, corine_meta)

        scols = int(soil_metadata["ncols"])
        srows = int(soil_metadata["nrows"])
        scellsize = int(soil_metadata["cellsize"])
        xllcorner = int(soil_metadata["xllcorner"])
        yllcorner = int(soil_metadata["yllcorner"])

        for srow in range(0, srows):
            #print(srow)
            for scol in range(0, scols):
                soil_id = soil_grid_template[srow, scol]
                if soil_id == -9999:
                    continue

                #get coordinate of clostest climate element of real soil-cell
                sh_gk5 = yllcorner + (scellsize / 2) + (srows - srow - 1) * scellsize
                sr_gk5 = xllcorner + (scellsize / 2) + scol * scellsize

                # check if current grid cell is used for agriculture                
                corine_id = corine_gk5_interpolate(sr_gk5, sh_gk5)
                if corine_id not in [2,3,4]:
                    soil_grid_template[srow, scol] = -9999

        print("filtered through CORINE")

    #set all data values to one, to count them later
    soil_grid_template[soil_grid_template != -9999] = 1
    #set all no-data values to 0, to ignore them while counting
    soil_grid_template[soil_grid_template == -9999] = 0

    #count cols in rows
    datacells_per_row = np.sum(soil_grid_template, axis=1)

    start_row = int(config["start-row"])
    end_row = int(config["end-row"])
    ncols = int(soil_metadata["ncols"])
    setup_id_to_ncs = {}

    def init_netcdfs(is_bgr):
        if is_bgr:
            ncs = {}
            # create a file per layer
            for layer in range(0, 20):
                path = config["scratch"] + "bgr/"
                if not os.path.exists(path):
                    os.makedirs(path)
                nc_file_path = path + f'bgr_{layer}.nc'
                if os.path.exists(nc_file_path):
                    rootgrp = Dataset(nc_file_path, "a", format="NETCDF4")
                else:
                    rootgrp = Dataset(nc_file_path, "w", format="NETCDF4")
                    rootgrp.history = "Created " + time.ctime()
                    rootgrp.createDimension("time", None) #appendable
                    rootgrp.createDimension("row", 875)
                    rootgrp.createDimension("col", 643)
                    sm = rootgrp.createVariable("sm", "i1", ("time", "row", "col"))
                    sm.description = "soil moisture"
                    sm.units = "%"
                    sm.missing_value = -99
                    st = rootgrp.createVariable("st", "i1", ("time", "row", "col"))
                    st.description = "soil temperature"
                    st.units = "°C"
                    st.missing_value = -99

                ncs[layer] = rootgrp
        else:
            path = config["scratch"] + "klimertrag/"
            if not os.path.exists(path):
                os.makedirs(path)
            nc_file_path = path + "klimertrag.nc"
            if os.path.exists(nc_file_path):
                rootgrp = Dataset(nc_file_path, "a", format="NETCDF4")
            else:
                rootgrp = Dataset(nc_file_path, "w", format="NETCDF4")
            ncs = rootgrp
            rootgrp.history = "Created " + time.ctime()
            rootgrp.createDimension("time", None) #appendable
            rootgrp.createDimension("row", 875)
            rootgrp.createDimension("col", 643)
            for stage in STAGES:
                sm = rootgrp.createVariable(stage + "_sm_0-30", "i1", ("time", "row", "col"))
                sm.description = "soil moisture"
                sm.units = "%"
                sm = rootgrp.createVariable(stage + "_sm_30-60", "i1", ("time", "row", "col"))
                sm.description = "soil moisture"
                sm.units = "%"
                sm = rootgrp.createVariable(stage + "_sm_60-90", "i1", ("time", "row", "col"))
                sm.description = "soil moisture"
                sm.units = "%"
                doy = rootgrp.createVariable(stage + "_doy", "i2", ("time", "row", "col"))
                doy.description = "day of year when event " + stage + " happend"
                doy.units = "days since start of year"
        
        return ncs

    def process_message(msg):
        if len(msg["errors"]) > 0:
            print("There were errors in message:", msg, "\nSkipping message!")
            return

        if not hasattr(process_message, "wnof_count"):
            process_message.wnof_count = 0
            process_message.setup_count = 0

        leave = False

        if not write_normal_output_files:
            custom_id = msg["customId"]
            setup_id = custom_id["setup_id"]
            is_bgr = custom_id["bgr"]

            if setup_id not in setup_id_to_ncs:
                setup_id_to_ncs[setup_id] = init_netcdfs(is_bgr)
            ncs = setup_id_to_ncs[setup_id]

            row = custom_id["srow"]
            col = custom_id["scol"]
            #crow = custom_id.get("crow", -1)
            #ccol = custom_id.get("ccol", -1)
            #soil_id = custom_id.get("soil_id", -1)

            debug_msg = f'received work result {process_message.received_env_count} customId: {msg.get("customId", "")}'
            print(debug_msg)
            #debug_file.write(debug_msg + "\n")

            process_message.received_env_count = process_message.received_env_count + 1
            write_output_to_netcdfs(row, col, msg.get("data", []), ncs, is_bgr)
                
        elif write_normal_output_files:

            if msg.get("type", "") in ["jobs-per-cell", "no-data", "setup_data"]:
                #print "ignoring", result.get("type", "")
                return

            print("received work result ", process_message.received_env_count, " customId: ", str(list(msg.get("customId", "").values())))

            custom_id = msg["customId"]
            setup_id = custom_id["setup_id"]
            row = custom_id["srow"]
            col = custom_id["scol"]
            #crow = custom_id.get("crow", -1)
            #ccol = custom_id.get("ccol", -1)
            #soil_id = custom_id.get("soil_id", -1)
            
            process_message.wnof_count += 1

            #with open("out/out-" + str(i) + ".csv", 'wb') as _:
            with open("out-normal/out-" + str(process_message.wnof_count) + ".csv", "w", newline='') as _:
                writer = csv.writer(_, delimiter=";")

                for data_ in msg.get("data", []):
                    results = data_.get("results", [])
                    orig_spec = data_.get("origSpec", "")
                    output_ids = data_.get("outputIds", [])

                    if len(results) > 0:
                        writer.writerow([orig_spec.replace("\"", "")])
                        for row in monica_io3.write_output_header_rows(output_ids,
                                                                      include_header_row=True,
                                                                      include_units_row=True,
                                                                      include_time_agg=False):
                            writer.writerow(row)

                        for row in monica_io3.write_output(output_ids, results):
                            writer.writerow(row)

                    writer.writerow([])

            process_message.received_env_count = process_message.received_env_count + 1

        return leave

    process_message.received_env_count = 1

    while not leave:
        try:
            #start_time_recv = timeit.default_timer()
            msg = socket.recv_json(encoding="latin-1")
            #elapsed = timeit.default_timer() - start_time_recv
            #print("time to receive message" + str(elapsed))
            #start_time_proc = timeit.default_timer()
            leave = process_message(msg)
            #elapsed = timeit.default_timer() - start_time_proc
            #print("time to process message" + str(elapsed))
        except zmq.error.Again as _e:
            print('no response from the server (with "timeout"=%d ms) ' % socket.RCVTIMEO)
            return
        except Exception as e:
            print("Exception:", e)
            #continue

    for _, ncs in setup_id_to_ncs.items():
        if isinstance(ncs, dict):
            for _, nc in ncs.items():
                nc.close()
        elif isinstance(ncs, Dataset):
            ncs.close()


    print("exiting run_consumer()")
    #debug_file.close()

if __name__ == "__main__":
    run_consumer()


