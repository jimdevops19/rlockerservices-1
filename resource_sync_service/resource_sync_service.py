import time
import os
import resource_sync_service.constants as const
from resource_sync_service.vsphere_base import Vsphere
from resource_sync_service import conf
from service_base.service_base import ServiceBase
from service_base.connection import ResourceLockerConnection
from service_base import utils, settings

rlocker = ResourceLockerConnection(conf_file=conf)


class ResourceSyncService(ServiceBase):
    def __init__(self):
        self.v = Vsphere(
            os.environ.get("VSPHERE_HOST") or conf["svc"].get("VSPHERE_HOST"),
            os.environ.get("VSPHERE_USER") or conf["svc"].get("VSPHERE_USER"),
            os.environ.get("VSPHERE_PASSWORD") or conf["svc"].get("VSPHERE_PASSWORD"),
        )
        if self.v:
            print({"VSPHERE_CONNECTION" : "OK"})

        self.dc_kwargs = {
            "dc" : "Datacenter-CP",
            "cluster" : "Cluster-1"
        }
        self.collection = [] # Maintain a list of metadata so it will be easier to aggregate data if needed

    def actual_vs_locked_status(self):
        """
        Method that compares the resource pools in the desired DC,
            and checks if a resource is locked
        Adds to the collection the results
        """
        pools = self.v.get_all_pools(**self.dc_kwargs)
        print(pools)
        for pool in pools:
            check_resource = rlocker.get_lockable_resources(signoff=pool)
            has_associated_lock = True if check_resource else False
            self.collection.append(
                {
                    "pool_name" : pool,
                    "has_associated_lock" : has_associated_lock
                }
            )

    def run(self):
        self.actual_vs_locked_status()
        for c in self.collection:
            print(c)

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Cleanup")
        time.sleep(int(conf["svc"].get("INTERVAL")))

    @staticmethod
    def run_prerequisites():
        pass
