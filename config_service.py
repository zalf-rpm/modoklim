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
from collections import defaultdict, deque
import json
import os
from pathlib import Path
import subprocess as sp
import sys
import time
from threading import Thread, Lock
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
common_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "common.capnp"), imports=abs_imports)
persistence_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "persistence.capnp"), imports=abs_imports)
service_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "service.capnp"), imports=abs_imports)
config_capnp = capnp.load("config.capnp", imports=abs_imports)

class LocalService(config_service_capnp.Service.Server, common.Identifiable, common.Persistable, serv.AdministrableService): 

    def __init__(self, no_of_configs=1, id=None, name=None, description=None, admin=None, restorer=None):
        common.Identifiable.__init__(self, id, name, description)
        common.Persistable.__init__(self, restorer)
        serv.AdministrableService.__init__(self, admin)

        self._restorer = restorer
        self._no_of_configs = no_of_configs
        self._petname_to_sturdy_refs = defaultdict(deque)
        self._petname_to_admin_service_caps = defaultdict(list)

        self._registrar = None
        self._registrar_sr = None


    def stop_services(self):
        proms = []
        for _, caps in self._petname_to_admin_service_caps.items():
            for cap in caps:
                proms.append(cap.cast_as(service_capnp.Admin).stop())
        return capnp.join_promises(proms)


    def start_services(self):
        if self.admin:
            self.admin.stop_action = self.stop_services

        self._registrar = Registrar(self)
        self._registrar_sr, _ = self._restorer.save(self._registrar)

        # start monicas
        for i in range(self._no_of_configs):
            print("monica", i)
            Thread(
                target=sp.run, 
                args=(["/home/berg/GitHub/monica/_cmake_linux_debug/monica-capnp-server", "-rsr", self._registrar_sr],)
            ).start()

        # start climate service
        Thread(
            target=sp.run, 
            args=(["python", "/home/berg/GitHub/mas-infrastructure/src/python/services/climate/dwd_germany_service.py"],),
            kwargs={
                "input": json.dumps({
                    "service": {"reg_sr": self._registrar_sr, "reg_name": "dwd_germany"},
                    "admin": {"reg_sr": self._registrar_sr, "reg_name": "dwd_germany", "cat_id": "admin"}
                }), "encoding": "ascii"}
        ).start()
        
        # start soil service
        Thread(
            target=sp.run, 
            args=(["python", "/home/berg/GitHub/mas-infrastructure/src/python/services/soil/sqlite_soil_data_service.py"],),
            kwargs={
                "input": json.dumps({
                    "service": {"reg_sr": self._registrar_sr, "reg_name": "buek_1000"},
                    "admin": {"reg_sr": self._registrar_sr, "reg_name": "buek_1000", "cat_id": "admin"}
                }), "encoding": "ascii"}
        ).start()
        
        # start dgm service
        Thread(
            target=sp.run, 
            args=([
                "python", 
                "/home/berg/GitHub/mas-infrastructure/src/python/services/grid/ascii_grid.py",
                "path_to_ascii_grid=/home/berg/GitHub/mas-infrastructure/data/geo/dem_1000_31469_gk5.asc",
                "grid_crs=gk5",
                "val_type=float"
            ],),
            kwargs={
                "input": json.dumps({
                    "service": {"reg_sr": self._registrar_sr, "reg_name": "dgm_1000"},
                    "admin": {"reg_sr": self._registrar_sr, "reg_name": "dgm_1000", "cat_id": "admin"}
                }), "encoding": "ascii"}
        ).start()

        # start slope service
        Thread(
            target=sp.run, 
            args=([
                "python", 
                "/home/berg/GitHub/mas-infrastructure/src/python/services/grid/ascii_grid.py",
                "path_to_ascii_grid=/home/berg/GitHub/mas-infrastructure/data/geo/slope_1000_31469_gk5.asc",
                "grid_crs=gk5",
                "val_type=float"
            ],),
            kwargs={
                "input": json.dumps({
                    "service": {"reg_sr": self._registrar_sr, "reg_name": "slope_1000"},
                    "admin": {"reg_sr": self._registrar_sr, "reg_name": "slope_1000", "cat_id": "admin"}
                }), "encoding": "ascii"}
        ).start()

        # start job factory
        Thread(
            target=sp.run, 
            args=([
                "python", 
                "/home/berg/GitHub/mas-infrastructure/src/python/services/jobs/jobs_service.py",
                "path_to_csv=/home/berg/Desktop/Koordinaten_HE_dummy_ID.csv"
            ],),
            kwargs={
                "input": json.dumps({
                    "service": {"reg_sr": self._registrar_sr, "reg_name": "jobs"},
                    "admin": {"reg_sr": self._registrar_sr, "reg_name": "jobs", "cat_id": "admin"}
                }), "encoding": "ascii"}
        ).start()


    def nextConfig_context(self, context): # createConfig @0 () -> (config :C, noFurtherConfigs :Bool = false);
        def rotate_and_get_first_value(dq):
            dq.rotate()
            return dq[0] if len(dq) > 0 else ""
        if self._no_of_configs > 0:
            entries = list([{
                "name": petname, 
                "sturdyRef": rotate_and_get_first_value(self._petname_to_sturdy_refs.get(petname, deque())) 
            } for petname in ["dwd_germany", "buek_1000", "dgm_1000", "slope_1000", "monica", "jobs"]])
            context.results.config = config_capnp.Config.new_message(entries=entries)
        else:
            context.results.noFurtherConfigs = True
        self._no_of_configs -= 1

#------------------------------------------------------------------------------

class Registrar(reg_capnp.Registrar.Server, common.Identifiable): 

    def __init__(self, service):
        self._service = service

    def register_context(self, context): # register @0 (cap :Common.Identifiable, regName :Text, categoryId :Text) -> (unreg :Common.Action, reregSR :Text);
        print("Registrator: register message received")
        regName = context.params.regName
        catId = context.params.categoryId
        cap = context.params.cap
        if catId == "admin":
            self._service._petname_to_admin_service_caps[regName].append(cap)
        else:
            return cap.cast_as(persistence_capnp.Persistent).save().then(
                lambda res: self._service._petname_to_sturdy_refs[regName].append(res.sturdyRef))

#------------------------------------------------------------------------------

async def main(no_of_configs=1, serve_bootstrap=True, host=None, port=None, 
    id=None, name="Jobs Service", description=None, use_async=False):

    config = {
        "no_of_configs": str(no_of_configs),
        "port": port, 
        "host": host,
        "id": id,
        "name": name,
        "description": description,
        "serve_bootstrap": serve_bootstrap,
        "use_async": use_async
    }
    # read commandline args only if script is invoked directly from commandline
    if len(sys.argv) > 1 and __name__ == "__main__":
        for arg in sys.argv[1:]:
            k, v = arg.split("=")
            if k in config:
                config[k] = v
    print(config)

    restorer = common.Restorer()
    service = LocalService(no_of_configs=int(config["no_of_configs"]), 
        id=config["id"], name=config["name"], description=config["description"], restorer=restorer)
    if config["use_async"]:
        await serv.async_init_and_run_service({"service": service}, config["host"], config["port"], 
            serve_bootstrap=config["serve_bootstrap"], restorer=restorer, 
            run_before_enter_eventloop=lambda: service.start_services())
    else:
        serv.init_and_run_service({"service": service}, config["host"], config["port"], 
            serve_bootstrap=config["serve_bootstrap"], restorer=restorer,
            run_before_enter_eventloop=lambda: service.start_services())

#------------------------------------------------------------------------------

if __name__ == '__main__':
    asyncio.run(main(no_of_configs=1, use_async=True)) 