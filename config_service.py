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
config_capnp = capnp.load("config.capnp", imports=abs_imports)

class LocalService(config_service_capnp.Service.Server, common.Identifiable, serv.AdministrableService): 

    def __init__(self, restorer, no_of_configs = 1):
        self._restorer = restorer
        self._no_of_configs = no_of_configs
        self._petname_to_sturdy_refs = defaultdict(list) 
        #self._processes_lock = Lock()
        #self._started_processes = []


    def __del__(self):
        pass
        #for p in self._started_processes:
        #    p.terminate()


    def start_services(self):

        #def start_monica(sr):
        #    p = sp.Popen(["/home/berg/GitHub/monica/_cmake_linux_debug/monica-capnp-server", "-d", "-rsr", sr])
        #    self._processes_lock.acquire()
        #    self._started_processes.append(p)
        #    self._processes_lock.release()

        # start monicas
        for i in range(self._no_of_configs):
            sr = self.create_registrar("monica")
            print("monica", i, "sr:", sr)
            Thread(
                target=sp.run, 
                args=(["/home/berg/GitHub/monica/_cmake_linux_debug/monica-capnp-server", "-d", "-rsr", sr],),
                #kwargs={"input": json.dumps({"service": sr}), "encoding": "ascii"}
            ).start()
            #Thread(target=start_monica, args=(sr,)).start()
            #self._started_processes.append(sp.Popen(
            #    ["/home/berg/GitHub/monica/_cmake_linux_debug/monica-capnp-server", "-d", "-rsr", sr]))

        #def start_python(script, sr):
        #    p = sp.Popen(["python", script], stdin=sp.PIPE, shell=False, encoding="ascii")
        #        #cwd="/home/berg/GitHub/mas-infrastructure")
        #    p.communicate(input=json.dumps({"service": sr}))
        #    print("after communicate")
        #    self._processes_lock.acquire()
        #    self._started_processes.append(p)
        #    self._processes_lock.release()

        # start climate service
        sr = self.create_registrar("dwd_germany")
        print("dwd_germany sr:", sr)
        Thread(
            target=sp.run, 
            args=(["python", "/home/berg/GitHub/mas-infrastructure/src/python/services/climate/dwd_germany_service.py"],),
            kwargs={"input": json.dumps({"service": sr}), "encoding": "ascii"}
        ).start()
        #Thread(target=start_python, args=("/home/berg/GitHub/mas-infrastructure/src/python/services/climate/dwd_germany_service.py", sr)).start()
        #p1 = sp.Popen(
        #    ["python", "/home/berg/GitHub/mas-infrastructure/src/python/services/climate/dwd_germany_service.py"],
        #    stdin=sp.PIPE, shell=False)#True,
            #cwd="/home/berg/GitHub/mas-infrastructure")
        #out, err = p1.communicate(input=json.dumps({"service": sr}).encode(encoding="ascii"))
        #print("dwd_germany out:", out, "err:", err)
        #p1.communicate(input=json.dumps({"service": sr}).encode(encoding="ascii"))
        #print("after communicate")
        #self._started_processes.append(p1)
        
        # start climate service
        sr = self.create_registrar("buek_1000")
        print("buek_1000 sr:", sr)
        Thread(
            target=sp.run, 
            args=(["python", "/home/berg/GitHub/mas-infrastructure/src/python/services/soil/sqlite_soil_data_service.py"],),
            kwargs={"input": json.dumps({"service": sr}), "encoding": "ascii"}
        ).start()
        #self._started_processes.append(sp.Popen(
        #    ["python", "src/python/services/soil/sqlite_soil_data_service.py"],
        #    input=json.dumps({"service": sr}), shell=True,
        #    cwd="/home/berg/GitHub/mas-infrastructure"))

        # start dgm service
        sr = self.create_registrar("dgm_1000")
        print("dgm_1000 sr:", sr)
        Thread(
            target=sp.run, 
            args=([
                "python", 
                "/home/berg/GitHub/mas-infrastructure/src/python/services/grid/ascii_grid.py",
                "path_to_ascii_grid=/home/berg/GitHub/mas-infrastructure/data/geo/dem_1000_31469_gk5.asc",
                "grid_crs=gk5",
                "val_type=float"
            ],),
            kwargs={"input": json.dumps({"service": sr}), "encoding": "ascii"}
        ).start()
        #self._started_processes.append(sp.Popen(
        #    [
        #        "python", 
        #        "src/python/services/grid/ascii_grid.py",
        #        "path_to_ascii_grid=data/geo/dem_1000_31469_gk5.asc",
        #        "grid_crs=gk5",
        #        "val_type=float"
        #    ],
        #    input=json.dumps({"service": sr}), shell=True,
        #    cwd="/home/berg/GitHub/mas-infrastructure"))

        # start slope service
        sr = self.create_registrar("slope_1000")
        print("slope_1000 sr:", sr)
        Thread(
            target=sp.run, 
            args=([
                "python", 
                "/home/berg/GitHub/mas-infrastructure/src/python/services/grid/ascii_grid.py",
                "path_to_ascii_grid=/home/berg/GitHub/mas-infrastructure/data/geo/slope_1000_31469_gk5.asc",
                "grid_crs=gk5",
                "val_type=float"
            ],),
            kwargs={"input": json.dumps({"service": sr}), "encoding": "ascii"}
        ).start()
        #self._started_processes.append(sp.Popen(
        #    [
        #        "python", 
        #        "src/python/services/grid/ascii_grid.py",
        #        "path_to_ascii_grid=data/geo/slope_1000_31469_gk5.asc",
        #        "grid_crs=gk5",
        #        "val_type=float"
        #    ],
        #    input=json.dumps({"service": sr}), shell=True,
        #    cwd="/home/berg/GitHub/mas-infrastructure"))

        # start job factory
        sr = self.create_registrar("jobs")
        print("slope_1000 sr:", sr)
        Thread(
            target=sp.run, 
            args=([
                "python", 
                "/home/berg/GitHub/mas-infrastructure/src/python/services/jobs/jobs_service.py",
                "path_to_csv=/home/berg/Desktop/Koordinaten_HE_dummy_ID.csv"
            ],),
            kwargs={"input": json.dumps({"service": sr}), "encoding": "ascii"}
        ).start()
        #self._started_processes.append(sp.Popen(
        #    [
        #        "python", 
        #        "src/python/services/jobs/jobs_service.py",
        #        "path_to_csv=/home/berg/Desktop/Koordinaten_HE_dummy_ID.csv"
        #    ],
        #    input=json.dumps({"service": sr}), shell=True,
        #    cwd="/home/berg/GitHub/mas-infrastructure"))


    def create_registrar(self, name):
        reg = Registrar()
        sr, unsave_sr = self._restorer.save(reg)
        if name in self._petname_to_sturdy_refs:
            self._petname_to_sturdy_refs = [self._petname_to_sturdy_refs[name]]
            reg.register_sr_action = lambda sr: self._petname_to_sturdy_refs[name].append(sr)
        else:
            reg.register_sr_action = lambda sr: self._petname_to_sturdy_refs.update(name=sr)
        #print("created registrar with sr:", sr)
        return sr


    def nextConfig_context(self, context): # createConfig @0 () -> (config :C, noFurtherConfigs :Bool = false);
        def val_or_pop(v):
            return v.pop() if isinstance(v, list) else v
        if self._no_of_configs > 0:
            entries = list([{
                "name": petname, 
                "sturdyRef": val_or_pop(self._petname_to_sturdy_refs.get(petname, ""))
            } for petname in ["dwd_germany", "buek_1000", "dgm_1000", "slope_1000", "monica", "jobs"]])
            context.results.config = config_capnp.Config.new_message(entries=entries)
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
        print("Registrator: register message received")
        if self._register_sr_action:
            return context.params.cap.cast_as(persistence_capnp.Persistent).save().then(lambda res: self._register_sr_action(res.sturdyRef))

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
            serve_bootstrap=config["serve_bootstrap"], restorer=restorer, 
            run_before_enter_eventloop=lambda: service.start_services())
    else:
        
        serv.init_and_run_service({"service": service}, config["host"], config["port"], 
            serve_bootstrap=config["serve_bootstrap"], restorer=restorer,
            run_before_enter_eventloop=lambda: service.start_services())

#------------------------------------------------------------------------------

if __name__ == '__main__':
    #asyncio.run(main(False, no_of_configs=1)) 
    asyncio.run(main(True, no_of_configs=1)) #asyncio