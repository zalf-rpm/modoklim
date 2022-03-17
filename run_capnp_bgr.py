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

import asyncio
import capnp
from collections import defaultdict
import csv
from datetime import date, timedelta
import json
import os
from pathlib import Path
from pyproj import CRS, Transformer
import sys
import time

import monica_io3
import monica_run_lib as Mrunlib

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent
if str(PATH_TO_REPO) not in sys.path:
    sys.path.insert(1, str(PATH_TO_REPO))

PATH_TO_PYTHON_CODE = PATH_TO_REPO.parent / "mas-infrastructure" / "src" / "python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

PATH_TO_CAPNP_SCHEMAS = PATH_TO_REPO.parent / "mas-infrastructure" / "capnproto_schemas"
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]

import common.capnp_async_helpers as async_helpers
import common.common as common

grid_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "grid.capnp"), imports=abs_imports)
soil_data_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "soil_data.capnp"), imports=abs_imports)
model_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "model.capnp"), imports=abs_imports)
climate_data_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "climate_data.capnp"), imports=abs_imports)
common_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "common.capnp"), imports=abs_imports)
#mgmt_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "management.capnp"), imports=abs_imports)
jobs_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "jobs.capnp"), imports=abs_imports)
config_service_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "config.capnp"), imports=abs_imports)
config_capnp = capnp.load("config.capnp")

DATA_GRID_CROPS = "germany/germany-complete_1000_25832_etrs89-utm32n.asc"
TEMPLATE_PATH_HARVEST = "{path_to_data_dir}/projects/monica-germany/ILR_SEED_HARVEST_doys_{crop_id}.csv"

# commandline parameters e.g "server=localhost port=6666 shared_id=2"
def run():
    config = {
        "sim.json": "sim_bgr.json",
        "crop.json": "crop_bgr.json",
        "site.json": "site.json",
        "setups-file": "sim_setups_capnp_bgr.csv",
        "pet2sr": "petname_to_sturdy_refs.json",
        "config_sr": "capnp://insecure@10.10.24.210:38989/0bf3f1ce-5026-4765-bbaf-22819f734d4e",
        "run-setups": "[1]"
    }
    
    # read commandline args only if script is invoked directly from commandline
    if len(sys.argv) > 1 and __name__ == "__main__":
        for arg in sys.argv[1:]:
            k, v = arg.split("=")
            if k in config:
                config[k] = v

    print("config:", config)

    # read setup from csv file
    setups = Mrunlib.read_sim_setups(config["setups-file"])
    run_setups = json.loads(config["run-setups"])
    print("read sim setups: ", config["setups-file"])

    # load petnames file
    with open(config["pet2sr"]) as _:
        pet_to_srs = json.load(_)

    conMan = common.ConnectionManager()

    def connect(conf, sr_or_petname, cast_as):
        sr = sr_or_petname if sr_or_petname[:8] == "capnp://" else conf.get(sr_or_petname, pet_to_srs[sr_or_petname])
        return conMan.try_connect(sr, cast_as=cast_as)

    
    #transforms geospatial coordinates from one coordinate reference system to another
    # transform wgs84 into gk5
    wgs84_crs = CRS.from_epsg(4326)
    utm32_crs = CRS.from_epsg(25832)
    utm32_to_latlon_transformer = Transformer.from_crs(utm32_crs, wgs84_crs, always_xy=True)

    ilr_seed_harvest_data = defaultdict(lambda: {"interpolate": None, "data": defaultdict(dict), "is-winter-crop": None})

    sent_env_count = 1
    start_time = time.perf_counter()

    config_service_cap = conMan.connect(config["config_sr"], config_service_capnp.Service)
    conf_res = config_service_cap.nextConfig().wait()
    if conf_res.noFurtherConfigs:
        print("no further configs available")
        return
    serv_conf = {}
    for e in conf_res.config.as_struct(config_capnp.Config).entries:
        if e.sturdyRef:
            serv_conf[e.name] = e.sturdyRef
    print(serv_conf)

    return

    listOfClimateFiles = set()
    # run calculations for each setup
    for _, setup_id in enumerate(run_setups):

        if setup_id not in setups:
            continue
        start_setup_time = time.perf_counter()      

        setup = setups[setup_id]
        crop_id = setup["crop-id"]

        ## extract crop_id from crop-id name that has possible an extenstion
        crop_id_short = crop_id.split('_')[0]

        #climate_dataset_cap = connect(conf.get("dwd_germany", setup["climate_dataset_sr"]), climate_data_capnp.Dataset)
        climate_service_cap = connect(serv_conf, setup["climate_dataset_sr"], climate_data_capnp.Service)
        climate_dataset_cap = climate_service_cap.getAvailableDatasets().wait().datasets[0].data
        soil_cap = connect(serv_conf, setup["soil_sr"], soil_data_capnp.Service)
        dgm_cap = connect(serv_conf, setup["dgm_sr"], grid_capnp.Grid)
        slope_cap = connect(serv_conf, setup["slope_sr"], grid_capnp.Grid)
        monica_cap = connect(serv_conf, setup["monica_sr"], model_capnp.EnvInstance)
        landcover_cap = connect(serv_conf, setup["landcover_sr"], grid_capnp.Grid) if setup["landcover"] else None
        jobs_cap = connect(serv_conf, setup["jobs_sr"], jobs_capnp.Service)

        # add crop id from setup file
        try:
            #read seed/harvest dates for each crop_id
            path_harvest = TEMPLATE_PATH_HARVEST.format(path_to_data_dir="data/",  crop_id=crop_id_short)
            print("created seed harvest gk5 interpolator and read data: ", path_harvest)
            Mrunlib.create_seed_harvest_geoGrid_interpolator_and_read_data(path_harvest, wgs84_crs, utm32_crs, ilr_seed_harvest_data)
        except IOError:
            path_harvest = TEMPLATE_PATH_HARVEST.format(path_to_data_dir="data",  crop_id=crop_id_short)
            print("Couldn't read file:", path_harvest)
            continue

        # read template sim.json 
        with open(setup.get("sim.json", config["sim.json"])) as _:
            sim_json = json.load(_)
        # change start and end date acording to setup
        if setup["start_date"]:
            sim_json["climate.csv-options"]["start-date"] = str(setup["start_date"])
        if setup["end_date"]:
            sim_json["climate.csv-options"]["end-date"] = str(setup["end_date"]) 
        #sim_json["include-file-base-path"] = paths["include-file-base-path"]
        # read template site.json 
        with open(setup.get("site.json", config["site.json"])) as _:
            site_json = json.load(_)

        # read template crop.json
        with open(setup.get("crop.json", config["crop.json"])) as _:
            crop_json = json.load(_)

        crop_json["CropParameters"]["__enable_vernalisation_factor_fix__"] = setup["use_vernalisation_fix"] if "use_vernalisation_fix" in setup else False

        # set the current crop used for this run id
        crop_json["cropRotation"][2] = crop_id

        # create environment template from json templates
        env_template = monica_io3.create_env_json_from_json_config({
            "crop": crop_json,
            "site": site_json,
            "sim": sim_json,
            "climate": ""
        })

        #coords = Mrunlib.read_csv(setup["coords.csv"], key="id")
        #for id, coord in coords.items():
        while True:
            j = jobs_cap.nextJob().wait().job

            if j.noFurtherJobs:
                break
        
            coord = json.loads(j.data.as_text())
            r = float(coord["x"])
            h = float(coord["y"])

            lon, lat = utm32_to_latlon_transformer.transform(r, h)

            llcoord = {"lat": lat, "lon": lon}
            timeseries_prom = climate_dataset_cap.closestTimeSeriesAt(llcoord)

            query = {"mandatory": ["soilType", "organicCarbon", "rawDensity"]}
            soil_profiles_prom = soil_cap.profilesAt(llcoord, query)
        
            dgm_prom = dgm_cap.closestValueAt(llcoord)
            slope_prom = slope_cap.closestValueAt(llcoord)

            landcover_prom = landcover_cap.closestValueAt(llcoord) if landcover_cap else None


            worksteps = env_template["cropRotation"][0]["worksteps"]
            sowing_ws = next(filter(lambda ws: ws["type"][-6:] == "Sowing", worksteps))
            harvest_ws = next(filter(lambda ws: ws["type"][-7:] == "Harvest", worksteps))

            ilr_interpolate = ilr_seed_harvest_data[crop_id_short]["interpolate"]
            seed_harvest_cs = ilr_interpolate(r, h) if ilr_interpolate else None

            # set external seed/harvest dates
            if seed_harvest_cs:
                seed_harvest_data = ilr_seed_harvest_data[crop_id_short]["data"][seed_harvest_cs]
                if seed_harvest_data:
                    is_winter_crop = ilr_seed_harvest_data[crop_id_short]["is-winter-crop"]

                    if setup["sowing-date"] == "fixed":  # fixed indicates that regionally fixed sowing dates will be used
                        sowing_date = seed_harvest_data["sowing-date"]
                    elif setup["sowing-date"] == "auto":  # auto indicates that automatic sowng dates will be used that vary between regions
                        sowing_date = seed_harvest_data["latest-sowing-date"]
                    elif setup["sowing-date"] == "fixed1":  # fixed1 indicates that a fixed sowing date will be used that is the same for entire germany
                        sowing_date = sowing_ws["date"]
                    

                    sds = [int(x) for x in sowing_date.split("-")]
                    sd = date(2001, sds[1], sds[2])
                    sdoy = sd.timetuple().tm_yday

                    if setup["harvest-date"] == "fixed":  # fixed indicates that regionally fixed harvest dates will be used
                        harvest_date = seed_harvest_data["harvest-date"]                         
                    elif setup["harvest-date"] == "auto":  # auto indicates that automatic harvest dates will be used that vary between regions
                        harvest_date = seed_harvest_data["latest-harvest-date"]
                    elif setup["harvest-date"] == "auto1":  # fixed1 indicates that a fixed harvest date will be used that is the same for entire germany
                        harvest_date = harvest_ws["latest-date"]

                    # print("sowing_date:", sowing_date, "harvest_date:", harvest_date)
                    # print("sowing_date:", sowing_ws["date"], "harvest_date:", sowing_ws["date"])

                    hds = [int(x) for x in harvest_date.split("-")]
                    hd = date(2001, hds[1], hds[2])
                    hdoy = hd.timetuple().tm_yday

                    esds = [int(x) for x in seed_harvest_data["earliest-sowing-date"].split("-")]
                    esd = date(2001, esds[1], esds[2])

                    # sowing after harvest should probably never occur in both fixed setup!
                    if setup["sowing-date"] == "fixed" and setup["harvest-date"] == "fixed":
                        #calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["date"] = seed_harvest_data["sowing-date"]
                        harvest_ws["date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        #print("dates: ", int(seed_harvest_cs), ":", sowing_ws["date"])
                        #print("dates: ", int(seed_harvest_cs), ":", harvest_ws["date"])
                    
                    elif setup["sowing-date"] == "fixed" and setup["harvest-date"] == "auto":
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["date"] = seed_harvest_data["sowing-date"]
                        harvest_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        #print("dates: ", int(seed_harvest_cs), ":", sowing_ws["date"])
                        #print("dates: ", int(seed_harvest_cs), ":", harvest_ws["latest-date"])

                    elif setup["sowing-date"] == "fixed" and setup["harvest-date"] == "auto1":
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy - 1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["date"] = seed_harvest_data["sowing-date"]
                        harvest_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], hds[1], hds[2])
                        print("dates: ", int(seed_harvest_cs), ":", sowing_ws["date"])
                        print("dates: ", int(seed_harvest_cs), ":", harvest_ws["latest-date"])

                    elif setup["sowing-date"] == "auto" and setup["harvest-date"] == "fixed":
                        sowing_ws["earliest-date"] = seed_harvest_data["earliest-sowing-date"] if esd > date(esd.year, 6, 20) else "{:04d}-{:02d}-{:02d}".format(sds[0], 6, 20)
                        calc_sowing_date = date(2000, 12, 31) + timedelta(days=max(hdoy+1, sdoy))
                        sowing_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(sds[0], calc_sowing_date.month, calc_sowing_date.day)
                        harvest_ws["date"] = seed_harvest_data["harvest-date"]
                        #print("dates: ", int(seed_harvest_cs), ":", sowing_ws["earliest-date"], "<", sowing_ws["latest-date"])
                        #print("dates: ", int(seed_harvest_cs), ":", harvest_ws["date"])

                    elif setup["sowing-date"] == "auto" and setup["harvest-date"] == "auto":
                        sowing_ws["earliest-date"] = seed_harvest_data["earliest-sowing-date"] if esd > date(esd.year, 6, 20) else "{:04d}-{:02d}-{:02d}".format(sds[0], 6, 20)
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["latest-date"] = seed_harvest_data["latest-sowing-date"]
                        harvest_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        #print("dates: ", int(seed_harvest_cs), ":", sowing_ws["earliest-date"], "<", sowing_ws["latest-date"])
                        #print("dates: ", int(seed_harvest_cs), ":", harvest_ws["latest-date"])

                    elif setup["sowing-date"] == "fixed1" and setup["harvest-date"] == "fixed":
                        #calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["date"] = sowing_date
                        # print(seed_harvest_data["sowing-date"])
                        harvest_ws["date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        #print("dates: ", int(seed_harvest_cs), ":", sowing_ws["date"])
                        #print("dates: ", int(seed_harvest_cs), ":", harvest_ws["date"])


                #print("dates: ", int(seed_harvest_cs), ":", sowing_ws["earliest-date"], "<", sowing_ws["latest-date"] )
                #print("dates: ", int(seed_harvest_cs), ":", harvest_ws["latest-date"], "<", sowing_ws["earliest-date"], "<", sowing_ws["latest-date"] )
                
                # print("dates: ", int(seed_harvest_cs), ":", sowing_ws["date"])
                # print("dates: ", int(seed_harvest_cs), ":", harvest_ws["date"])

            proms = [timeseries_prom, soil_profiles_prom, dgm_prom, slope_prom]
            if landcover_prom: 
                proms.append(landcover_prom)
            vals = capnp.join_promises(proms).wait()
            timeseries, soil_profiles, height_nn, slope = vals[:4]

            # check if current grid cell is used for agriculture                
            if landcover_prom and vals[4].val not in [2,3,4]:
                continue

            env_template["params"]["userCropParameters"]["__enable_T_response_leaf_expansion__"] = setup["LeafExtensionModifier"]
                
            # setting groundwater level
            if False and setup["groundwater-level"]:
                groundwaterlevel = 20
                layer_depth = 0
                for layer in soil_profile:
                    if layer.get("is_in_groundwater", False):
                        groundwaterlevel = layer_depth
                        #print("setting groundwaterlevel of soil_id:", str(soil_id), "to", groundwaterlevel, "m")
                        break
                    layer_depth += Mrunlib.get_value(layer["Thickness"])
                env_template["params"]["userEnvironmentParameters"]["MinGroundwaterDepthMonth"] = 3
                env_template["params"]["userEnvironmentParameters"]["MinGroundwaterDepth"] = [max(0, groundwaterlevel - 0.2) , "m"]
                env_template["params"]["userEnvironmentParameters"]["MaxGroundwaterDepth"] = [groundwaterlevel + 0.2, "m"]
                
            # setting impenetrable layer
            if False and setup["impenetrable-layer"]:
                impenetrable_layer_depth = Mrunlib.get_value(env_template["params"]["userEnvironmentParameters"]["LeachingDepth"])
                layer_depth = 0
                for layer in soil_profile:
                    if layer.get("is_impenetrable", False):
                        impenetrable_layer_depth = layer_depth
                        #print("setting leaching depth of soil_id:", str(soil_id), "to", impenetrable_layer_depth, "m")
                        break
                    layer_depth += Mrunlib.get_value(layer["Thickness"])
                env_template["params"]["userEnvironmentParameters"]["LeachingDepth"] = [impenetrable_layer_depth, "m"]
                env_template["params"]["siteParameters"]["ImpenetrableLayerDepth"] = [impenetrable_layer_depth, "m"]

            if setup["elevation"]:
                env_template["params"]["siteParameters"]["heightNN"] = height_nn.val.f

            if setup["slope"]:
                env_template["params"]["siteParameters"]["slope"] = slope.val.f / 100.0

            if setup["latitude"]:
                env_template["params"]["siteParameters"]["Latitude"] = lat

            if setup["CO2"]:
                env_template["params"]["userEnvironmentParameters"]["AtmosphericCO2"] = float(setup["CO2"])

            if setup["O3"]:
                env_template["params"]["userEnvironmentParameters"]["AtmosphericO3"] = float(setup["O3"])

            if setup["FieldConditionModifier"]:
                env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["species"]["FieldConditionModifier"] = float(setup["FieldConditionModifier"])

            if setup["StageTemperatureSum"]:
                stage_ts = setup["StageTemperatureSum"].split('_')
                stage_ts = [int(temp_sum) for temp_sum in stage_ts]
                orig_stage_ts = env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"][
                    "StageTemperatureSum"][0]
                if len(stage_ts) != len(orig_stage_ts):
                    stage_ts = orig_stage_ts
                    print('The provided StageTemperatureSum array is not '
                            'sufficiently long. Falling back to original StageTemperatureSum')

                env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"][
                    "StageTemperatureSum"][0] = stage_ts

            env_template["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = setup["fertilization"]
            env_template["params"]["simulationParameters"]["UseAutomaticIrrigation"] = setup["irrigation"]

            env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = setup["NitrogenResponseOn"]
            env_template["params"]["simulationParameters"]["WaterDeficitResponseOn"] = setup["WaterDeficitResponseOn"]
            env_template["params"]["simulationParameters"]["EmergenceMoistureControlOn"] = setup["EmergenceMoistureControlOn"]
            env_template["params"]["simulationParameters"]["EmergenceFloodingControlOn"] = setup["EmergenceFloodingControlOn"]

            env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]

            env_template["customId"] = {
                "setup_id": setup_id,
                "lat": lat, "lon": lon
            }

            #print("Harvest type:", setup["harvest-date"])
            #print("lat: ", env_template["customId"]["lat"], "lon:", env_template["customId"]["lon"])
            harvest_ws = next(filter(lambda ws: ws["type"][-7:] == "Harvest", env_template["cropRotation"][0]["worksteps"]))
            #if setup["harvest-date"] == "fixed":
            #    print("Harvest-date:", harvest_ws["date"])
            #elif setup["harvest-date"] == "auto":
            #    print("Harvest-date:", harvest_ws["latest-date"])

            #print(soil_profiles.profiles[0])
            res_str = monica_cap.run({
                "rest": common_capnp.StructuredText.new_message(value=json.dumps(env_template), structure={"json": None}), 
                "timeSeries": timeseries.timeSeries,
                #"soilProfile": soil_profiles.profiles[0]
            }).wait().result.as_struct(common_capnp.StructuredText).value

            write_monica_csv(json.loads(res_str), id=sent_env_count)

            sent_env_count += 1

        stop_setup_time = time.perf_counter()
        print("Setup ", (sent_env_count-1), " envs took ", (stop_setup_time - start_setup_time), " seconds")

    stop_time = time.perf_counter()

    try:
        print("sending ", (sent_env_count-1), " envs took ", (stop_time - start_time), " seconds")
        #print("ran from ", start, "/", row_cols[start], " to ", end, "/", row_cols[end]
        print("exiting run_producer()")
    except Exception:
        raise


def write_monica_csv(result, id):

    with open("out/test_" + str(id) + ".csv", "w", newline="") as _:
        writer = csv.writer(_, delimiter=",")

        for data_ in result.get("data", []):
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


if __name__ == "__main__":
    run()