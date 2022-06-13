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

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent.parent
if str(PATH_TO_REPO) not in sys.path:
    sys.path.insert(1, str(PATH_TO_REPO))

PATH_TO_PYTHON_CODE = PATH_TO_REPO.parent / "mas-infrastructure" / "src" / "python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

PATH_TO_CAPNP_SCHEMAS = PATH_TO_REPO.parent / "mas-infrastructure" / "capnproto_schemas"
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]

import monica_io3
import monica_run_lib as Mrunlib

import common.common as common

grid_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "grid.capnp"), imports=abs_imports)
soil_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "soil_data.capnp"), imports=abs_imports)
model_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "model.capnp"), imports=abs_imports)
climate_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "climate_data.capnp"), imports=abs_imports)
common_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "common.capnp"), imports=abs_imports)
mgmt_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "management.capnp"), imports=abs_imports)
jobs_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "jobs.capnp"), imports=abs_imports)
config_service_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "config.capnp"), imports=abs_imports)
geo_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "geo_coord.capnp"), imports=abs_imports)
bgr_capnp = capnp.load("bgr.capnp")

DATA_GRID_CROPS = "germany/germany-complete_1000_25832_etrs89-utm32n.asc"

#------------------------------------------------------------------------------

config = {
    "in_sr": None, # string
    "out_sr": None, # utm_coord + id attr
    #"sim.json": "sim_bgr.json",
    #"crop.json": "crop_bgr.json",
    #"site.json": "site.json",
    "dgm_attr": "dgm",
    "slope_attr": "slope",
    "climate_attr": "climate",
    "soil_attr": "soil",
    "coord_attr": "latlon",
    "setup_attr": "setup",
    "id_attr": "id",
    "ilr_attr": "ilr",
}
common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

conman = common.ConnectionManager()
inp = conman.try_connect(config["in_sr"], cast_as=common_capnp.Channel.Reader, retry_secs=1)
outp = conman.try_connect(config["out_sr"], cast_as=common_capnp.Channel.Writer, retry_secs=1)

wgs84_crs = CRS.from_epsg(4326)
utm32_crs = CRS.from_epsg(25832)
utm32_to_latlon_transformer = Transformer.from_crs(utm32_crs, wgs84_crs, always_xy=True)

scenario = ""#setup["scenario"]

def create_env(sim, crop, site, crop_id):
    if not hasattr(create_env, "cache"):
        create_env.cache = {}

    scsc = (sim, crop, site, crop_id)

    if scsc in create_env.cache:
        return create_env.cache[scsc]

    with open(sim) as _:
        sim_json = json.load(_)

    with open(site) as _:
        site_json = json.load(_)
    #if len(scenario) > 0 and scenario[:3].lower() == "rcp":
    #    site_json["EnvironmentParameters"]["rcp"] = scenario

    with open(crop) as _:
        crop_json = json.load(_)

    # set the current crop used for this run id
    crop_json["cropRotation"][2] = crop_id

    # create environment template from json templates
    env_template = monica_io3.create_env_json_from_json_config({
        "crop": crop_json,
        "site": site_json,
        "sim": sim_json,
        "climate": ""
    })

    create_env.cache[scsc] = env_template
    return env_template

try:
    if inp and outp:
        while True:
            in_msg = inp.read().wait()
            # check for end of data from in port
            if in_msg.which() == "done":
                break
            
            in_ip = in_msg.value.as_struct(common_capnp.IP)
            llcoord = common.get_fbp_attr(in_ip, config["coord_attr"]).as_struct(geo_capnp.LatLonCoord)
            height_nn = common.get_fbp_attr(in_ip, config["dgm_attr"]).as_struct(grid_capnp.Grid.Value).f
            slope = common.get_fbp_attr(in_ip, config["slope_attr"]).as_struct(grid_capnp.Grid.Value).f
            timeseries = common.get_fbp_attr(in_ip, config["climate_attr"]).as_interface(climate_capnp.TimeSeries)
            soil_profile = common.get_fbp_attr(in_ip, config["soil_attr"]).as_struct(soil_capnp.Profile)
            setup = common.get_fbp_attr(in_ip, config["setup_attr"]).as_struct(bgr_capnp.Setup)
            ilr = common.get_fbp_attr(in_ip, config["ilr_attr"]).as_struct(mgmt_capnp.ILRDates)
            id = common.get_fbp_attr(in_ip, config["id_attr"]).as_text()

            if len(soil_profile.layers) == 0:
                continue

            env_template = create_env(setup.simJson, setup.cropJson, setup.siteJson, setup.cropId)
                
            env_template["params"]["userCropParameters"]["__enable_vernalisation_factor_fix__"] = setup.useVernalisationFix

            worksteps = env_template["cropRotation"][0]["worksteps"]
            sowing_ws = next(filter(lambda ws: ws["type"][-6:] == "Sowing", worksteps))
            if ilr._has("sowing"):
                s = ilr.sowing
                sowing_ws["date"] = "{:04d}-{:02d}-{:02d}".format(s.year, s.month, s.day)
            if ilr._has("earliestSowing"):
                s = ilr.earliestSowing
                sowing_ws["earliest-date"] = "{:04d}-{:02d}-{:02d}".format(s.year, s.month, s.day)
            if ilr._has("latestSowing"):
                s = ilr.latestSowing
                sowing_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(s.year, s.month, s.day)

            harvest_ws = next(filter(lambda ws: ws["type"][-7:] == "Harvest", worksteps))
            if ilr._has("harvest"):
                h = ilr.harvest
                harvest_ws["date"] = "{:04d}-{:02d}-{:02d}".format(h.year, h.month, h.day)
            if ilr._has("latestHarvest"):
                h = ilr.latestHarvest
                harvest_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(h.year, h.month, h.day)


            env_template["params"]["userCropParameters"]["__enable_T_response_leaf_expansion__"] = setup.leafExtensionModifier
                
            #print("soil:", soil_profile)
            #env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile.layers

            # setting groundwater level
            if False and setup.groundwaterLevel:
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
            if False and setup.impenetrableLayer:
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

            if setup.elevation:
                env_template["params"]["siteParameters"]["heightNN"] = height_nn

            if setup.slope:
                env_template["params"]["siteParameters"]["slope"] = slope / 100.0

            if setup.latitude:
                env_template["params"]["siteParameters"]["Latitude"] = llcoord.lat

            if setup.co2 > 0:
                env_template["params"]["userEnvironmentParameters"]["AtmosphericCO2"] = setup.co2

            if setup.o3 > 0:
                env_template["params"]["userEnvironmentParameters"]["AtmosphericO3"] = setup.o3

            if setup.fieldConditionModifier:
                env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["species"]["FieldConditionModifier"] = setup.fieldConditionModifier

            if len(setup.stageTemperatureSum) > 0:
                stage_ts = setup.stageTemperatureSum.split('_')
                stage_ts = [int(temp_sum) for temp_sum in stage_ts]
                orig_stage_ts = env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"][
                    "StageTemperatureSum"][0]
                if len(stage_ts) != len(orig_stage_ts):
                    stage_ts = orig_stage_ts
                    print('The provided StageTemperatureSum array is not '
                            'sufficiently long. Falling back to original StageTemperatureSum')

                env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"][
                    "StageTemperatureSum"][0] = stage_ts

            env_template["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = setup.fertilization
            env_template["params"]["simulationParameters"]["UseAutomaticIrrigation"] = setup.irrigation

            env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = setup.nitrogenResponseOn
            env_template["params"]["simulationParameters"]["WaterDeficitResponseOn"] = setup.waterDeficitResponseOn
            env_template["params"]["simulationParameters"]["EmergenceMoistureControlOn"] = setup.emergenceMoistureControlOn
            env_template["params"]["simulationParameters"]["EmergenceFloodingControlOn"] = setup.emergenceFloodingControlOn

            env_template["customId"] = {
                "setup_id": setup.runId,
                "id": id,
                "crop_id": setup.cropId,
                "lat": llcoord.lat, "lon": llcoord.lon
            }

            capnp_env = model_capnp.Env.new_message()
            capnp_env.timeSeries = timeseries
            capnp_env.soilProfile = soil_profile
            capnp_env.rest = common_capnp.StructuredText.new_message(value=json.dumps(env_template), structure={"json": None})

            out_ip = common_capnp.IP.new_message(content=capnp_env, attributes=[{"key": "id", "value": id}])
            outp.write(value=out_ip).wait()

        # close out port
        outp.write(done=None).wait()

except Exception as e:
    print("create_bgr_env.py ex:", e)

print("create_bgr_env.py: exiting run")
