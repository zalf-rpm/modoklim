#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg-mohnicke@zalf.de>
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
import json
import os
from pathlib import Path
import subprocess as sp
import sys
import time
import uuid

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent
if str(PATH_TO_REPO) not in sys.path:
    sys.path.insert(1, str(PATH_TO_REPO))

PATH_TO_PYTHON_CODE = PATH_TO_REPO.parent / "mas-infrastructure" / "src" / "python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

import common.capnp_async_helpers as async_helpers
import common.common as common
import common.service as serv
import common.csv as csv

PATH_TO_CAPNP_SCHEMAS = PATH_TO_REPO.parent / "mas-infrastructure" / "capnproto_schemas"
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]
reg_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "registry.capnp"), imports=abs_imports)
config_service_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "config.capnp"), imports=abs_imports)
config_capnp = capnp.load("config.capnp", imports=abs_imports)

class LocalService(config_service_capnp.Service.Server, common.Identifiable, serv.AdministrableService): 

    def __init__(self, restorer, no_of_configs = 1):
        self._restorer = restorer
        self._no_of_configs = no_of_configs
        self._name_to_sturdy_refs = defaultdict(list) 

        #self.start_services()


    def start_services(self):

        print(self.create_registrar("monica"))
        return

        # start monicas
        for i in range(self._no_of_configs):
            sr = self.create_registrar("monica")
            sp.run(["/home/berg/GitHub/monica/_cmake_linux_debug/monica-capnp-server", "-rsr", sr])
            
        # start climate service
        sr = self.create_registrar("dwd_germany")
        sp.run(["python", "src/python/services/climate/dwd_germany_service.py"],
            input=json.dumps({"service": sr}), shell=True,
            cwd="/home/berg/GitHub/mas-infrastructure")

        # start climate service
        sr = self.create_registrar("buek_1000")
        sp.run(["python", "src/python/services/soil/sqlite_soil_data_service.py"],
            input=json.dumps({"service": sr}), shell=True,
            cwd="/home/berg/GitHub/mas-infrastructure")

        # start dgm service
        sr = self.create_registrar("dgm_1000")
        sp.run([
                "python", 
                "src/python/services/grid/ascii_grid.py",
                "path_to_ascii_grid=data/geo/dem_1000_31469_gk5.asc",
                "grid_crs=gk5",
                "val_type=float"
            ],
            input=json.dumps({"service": sr}), shell=True,
            cwd="/home/berg/GitHub/mas-infrastructure")

        # start slope service
        sr = self.create_registrar("slope_1000")
        sp.run([
                "python", 
                "src/python/services/grid/ascii_grid.py",
                "path_to_ascii_grid=data/geo/slope_1000_31469_gk5.asc",
                "grid_crs=gk5",
                "val_type=float"
            ],
            input=json.dumps({"service": sr}), shell=True,
            cwd="/home/berg/GitHub/mas-infrastructure")

        # start job factory
        sr = self.create_registrar("jobs")
        sp.run([
                "python", 
                "src/python/services/jobs/jobs_service.py",
                "path_to_csv=/home/berg/Desktop/Koordinaten_HE_dummy_ID.csv"
            ],
            input=json.dumps({"service": sr}), shell=True,
            cwd="/home/berg/GitHub/mas-infrastructure")


    def create_registrar(self, name):
        reg = Registrar()
        sr, unsave_sr = self._restorer.save(reg)
        if name in self._name_to_sturdy_refs:
            self._name_to_sturdy_refs = [self._name_to_sturdy_refs[name]]
            reg.register_action = lambda sr: self._name_to_sturdy_refs[name].append(sr)
        else:
            reg.register_action = lambda sr: self._name_to_sturdy_refs.insert(name, sr)
        return sr


    def createConfig_context(self, context): # createConfig @0 () -> (config :C, noFurtherConfigs :Bool = false);

        if self._no_of_configs > 0:
            context.results.config = config_capnp.Config.new_message(
                climateServiceSR=self._name_to_sturdy_refs.get("dwd_germany", ""),
                soilServiceSR=self._name_to_sturdy_refs.get("buek_1000", ""),
                dgmSR=self._name_to_sturdy_refs.get("dgm_1000", ""),
                slopeSR=self._name_to_sturdy_refs.get("slope_1000", ""),
                monicaSR=self._name_to_sturdy_refs.get("monica", "").pop(),
                jobFactorySR=self._name_to_sturdy_refs.get("jobs", "")
            )
        else:
            context.results.noFurtherConfigs = True

#------------------------------------------------------------------------------

class Registrar(reg_capnp.Registrar.Server, common.Identifiable): 

    def __init__(self, register_sr_action=None):
        self._register_sr_action = register_sr_action

    @property
    def register_sr_action(self):
        return self._register_action 

    @register_sr_action.setter
    def register_sr_action(self, a):
        self._register_sr_action = a 

    def register_context(self, context): # register @0 (cap :Common.Identifiable, regName :Text, categoryId :Text) -> (unreg :Common.Action, reregSR :Text);
        if self._register_sr_action:
            context.params.cap.save().then(lambda res: self._register_sr_action(res.sr))

#------------------------------------------------------------------------------

async def main(use_async, no_of_configs=1, serve_bootstrap=True, host=None, port=None, id=None, name="Jobs Service", description=None):

    config = {
        "no_of_configs": str(no_of_configs),
        "port": port, 
        "host": host,
        "id": id,
        "name": name,
        "description": description,
        "serve_bootstrap": str(serve_bootstrap)
    }
    # read commandline args only if script is invoked directly from commandline
    if len(sys.argv) > 1 and __name__ == "__main__":
        for arg in sys.argv[1:]:
            k, v = arg.split("=")
            if k in config:
                config[k] = v
    print(config)

    restorer = common.Restorer()
    service = LocalService(restorer, no_of_configs=int(config["no_of_configs"]))
    if use_async:
        await serv.async_init_and_run_service({"service": service}, config["host"], config["port"], 
        serve_bootstrap=config["serve_bootstrap"], restorer=restorer)
    else:
        
        serv.init_and_run_service({"service": service}, config["host"], config["port"], 
            serve_bootstrap=config["serve_bootstrap"], restorer=restorer)

#------------------------------------------------------------------------------

if __name__ == '__main__':
    asyncio.run(main(False, no_of_configs=2)) 
    #asyncio.run(main(True, no_of_configs=2)) #asyncio