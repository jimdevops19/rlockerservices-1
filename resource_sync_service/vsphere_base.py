import logging
import os
import ssl

import atexit

from pyVmomi import vim, vmodl
from pyVim.task import WaitForTask, WaitForTasks
from pyVim.connect import Disconnect, SmartStubAdapter, VimSessionOrientedStub

logger = logging.getLogger(__name__)


class Vsphere(object):
    """
    wrapper for vSphere
    """

    def __init__(self, host, user, password, port=443):
        """
        Initialize the variables required for vCenter server

        Args:
             host (str): Host name
             user (str): User name
             password (): Password for the Host
             port (int): Port number

        """
        self._host = host
        self._user = user
        self._password = password
        self._port = port
        self.sslContext = ssl._create_unverified_context()
        self._si = self._get_service_instance()

    def _get_service_instance(self):
        """
        Gets the service instance

        Returns:
            vim.ServiceInstance: Service Instance for Host

        """
        try:
            smart_stub = SmartStubAdapter(
                host=self._host,
                port=int(self._port),
                sslContext=self.sslContext,
                connectionPoolTimeout=0,
            )
            session_stub = VimSessionOrientedStub(
                smart_stub,
                VimSessionOrientedStub.makeUserLoginMethod(self._user, self._password),
            )
            service_instance = vim.ServiceInstance("ServiceInstance", session_stub)

            # Ensure connection to server is closed on program exit
            atexit.register(Disconnect, service_instance)
            return service_instance
        except vmodl.MethodFault as error:
            logger.error(f"Caught vmodl fault : {error.msg}")
            raise

    @property
    def get_content(self):
        """
        Retrieves the content

        Returns:
            vim.ServiceInstanceContent: Service Instance Content for Host

        """
        return self._si.RetrieveContent()

    @property
    def get_search_index(self):
        """
        Get the search index

        Returns:
            vim.SearchIndex: Instance of Search Index

        """
        return self.get_content.searchIndex

    def get_all_objs(self, content, vimtype, folder=None, recurse=True):
        """
        Generate objects of type vimtype

        Args:
            content (vim.ServiceInstanceContent): Service Instance Content
            vimtype (vim.type): Type of vim
                (e.g: For VM's, type is vim.VirtualMachine
                For Hosts, type is vim.HostSystem)
            folder (str): Folder name
            recurse (bool): True for recursive search

        Returns:
            dict: Dictionary of objects and corresponding name
               e.g:{
                   'vim.Datastore:datastore-12158': 'datastore1 (1)',
                   'vim.Datastore:datastore-12157': 'datastore1 (2)'
                   }

        """
        if not folder:
            folder = content.rootFolder

        obj = {}
        container = content.viewManager.CreateContainerView(folder, vimtype, recurse)
        for managed_object_ref in container.view:
            obj.update({managed_object_ref: managed_object_ref.name})
        container.Destroy()
        return obj

    def find_object_by_name(self, content, name, obj_type, folder=None, recurse=True):
        """
        Finds object by given name

        Args:
            content (vim.ServiceInstanceContent): Service Instance Content
            name (str): Name to search
            obj_type (list): list of vim.type
                (e.g: For VM's, type is vim.VirtualMachine
                For Hosts, type is vim.HostSystem)
            folder (str): Folder name
            recurse (bool): True for recursive search

        Returns:
            vim.type: Type of vim instance
            None: If vim.type doesn't exists

        """
        if not isinstance(obj_type, list):
            obj_type = [obj_type]

        objects = self.get_all_objs(content, obj_type, folder=folder, recurse=recurse)
        for obj in objects:
            if obj.name == name:
                return obj

        return None

    def get_vm_by_ip(self, ip, dc, vm_search=True):
        """
        Gets the VM using IP address

        Args:
            ip (str): IP address
            dc (str): Datacenter name
            vm_search (bool): Search for VMs if True, Hosts if False

        Returns:
            vim.VirtualMachine: VM instance

        """
        return self.get_search_index.FindByIp(
            datacenter=self.get_dc(dc), ip=str(ip), vmSearch=vm_search
        )

    def get_dc(self, name):
        """
        Gets the Datacenter

        Args:
            name (str): Datacenter name

        Returns:
            vim.Datacenter: Datacenter instance

        """
        for dc in self.get_content.rootFolder.childEntity:
            if dc.name == name:
                return dc

    def get_cluster(self, name, dc):
        """
        Gets the cluster

        Args:
            name (str): Cluster name
            dc (str): Datacenter name

        Returns:
            vim.ClusterComputeResource: Cluster instance

        """
        for cluster in self.get_dc(dc).hostFolder.childEntity:
            if cluster.name == name:
                return cluster

    def get_pool(self, name, dc, cluster):
        """
        Gets the Resource pool

        Args:
            name (str): Resource pool name
            dc (str): Datacenter name
            cluster (str): Cluster name

        Returns:
            vim.ResourcePool: Resource pool instance

        """
        cluster_obj = self.get_cluster(cluster, dc)
        for rp in cluster_obj.resourcePool.resourcePool:
            if rp.name == name:
                return rp

    def get_all_vms_in_pool(self, name, dc, cluster):
        """
        Gets all VM's in Resource pool

        Args:
            name (str): Resource pool name
            dc (str): Datacenter name
            cluster (str): Cluster name

        Returns:
            list: VM instances (vim.VirtualMachine)

        Raises:
            ResourcePoolNotFound: when Resource pool doesn't exist

        """
        rp = self.get_pool(name, dc, cluster)
        if not self.is_resource_pool_exist(name, dc, cluster):
            raise Exception("Resource Not Found")
        return [vm for vm in rp.vm]

    def get_vm_in_pool_by_name(self, name, dc, cluster, pool):
        """
        Gets the VM instance in a resource pool

        Args:
            name (str): VM name
            dc (str): Datacenter name
            cluster (str): Cluster name
            pool (str): pool name

        Returns:
            vim.VirtualMachine: VM instances

        """
        vms = self.get_all_vms_in_pool(pool, dc, cluster)
        for vm in vms:
            if vm.name == name:
                return vm

    def get_vm_power_status(self, vm):
        """
        Get the VM power status

        Args:
            vm (vm): VM object

        Returns:
            str: VM power status

        """
        return vm.summary.runtime.powerState

    def get_vms_ips(self, vms):
        """
        Get VMs IPs

        Args:
            vms (list): VM (vm) objects

        Returns:
            list: VMs IPs

        """
        return [vm.summary.guest.ipAddress for vm in vms]

    def is_resource_pool_exist(self, pool, dc, cluster):
        """
        Check whether resource pool exists in cluster or not

        Args:
            pool (str): Resource pool name
            dc (str): Datacenter name
            cluster (str): Cluster name

        Returns:
            bool: True if resource pool exists, otherwise False

        """
        return True if self.get_pool(pool, dc, cluster) else False

    def get_all_pools(self, dc, cluster):
        """
        Give all the resource pools in Cluster

        Args:
            dc (str): Datacenter name
            cluster (str): Cluster name

        Returns:
            list: All the resource pools

        """
        pools = []
        cluster_obj = self.get_cluster(cluster, dc)
        for rp in cluster_obj.resourcePool.resourcePool:
            pools.append(rp.name)

        return sorted(pools)

    def is_resource_pool_prefix_exist(self, pool_prefix, dc, cluster):
        """
        Check whether or not resource pool with the provided prefix exist

        Args:
            pool_prefix (str): The prefix to look for
            dc (str): Datacenter name
            cluster (str): Cluster name

        Returns:
            bool: True if a resource pool with the same name prefix exists, False otherwise

        """
        cluster_obj = self.get_cluster(cluster, dc)
        for rp in cluster_obj.resourcePool.resourcePool:
            if rp.name.startswith(pool_prefix):
                return True
        return False

    def poweroff_vms(self, vms):
        """
        Powers off the VM and wait for operation to complete

        Args:
            vms (list): VM instance list

        """
        to_poweroff_vms = []
        for vm in vms:
            status = self.get_vm_power_status(vm)
            logger.info(f"power state of {vm.name}: {status}")
            if status == "poweredOn":
                to_poweroff_vms.append(vm)
        logger.info(f"Powering off VMs: {[vm.name for vm in to_poweroff_vms]}")
        tasks = [vm.PowerOff() for vm in to_poweroff_vms]
        WaitForTasks(tasks, self._si)

    def poweron_vms(self, vms):
        """
        Powers on the VM and wait for operation to complete

        Args:
            vms (list): VM instance list

        """
        to_poweron_vms = []
        for vm in vms:
            status = self.get_vm_power_status(vm)
            logger.info(f"power state of {vm.name}: {status}")
            if status == "poweredOff":
                to_poweron_vms.append(vm)
        logger.info(f"Powering on VMs: {[vm.name for vm in to_poweron_vms]}")
        tasks = [vm.PowerOn() for vm in to_poweron_vms]
        WaitForTasks(tasks, self._si)

    def destroy_vms(self, vms):
        """
        Destroys the VM's

        Args:
             vms (list): VM instance list

        """
        self.poweroff_vms(vms)
        logger.info(f"Destroying VM's: {[vm.name for vm in vms]}")
        tasks = [vm.Destroy_Task() for vm in vms]
        WaitForTasks(tasks, self._si)

    def remove_vms_from_inventory(self, vms):
        """
        Remove the VM's from inventory

        Args:
            vms (list): VM instance list

        """
        self.poweroff_vms(vms)
        for vm in vms:
            logger.info(f"Removing VM from inventory: {vm.name}")
            vm.UnregisterVM()

    def destroy_pool(self, pool, dc, cluster):
        """
        Deletes the Resource Pool

        Args:
            pool (str): Resource pool name
            dc (str): Datacenter name
            cluster (str): Cluster name

        """
        vms_in_pool = self.get_all_vms_in_pool(pool, dc, cluster)
        logger.info(f"VM's in resource pool {pool}: {[vm.name for vm in vms_in_pool]}")
        self.destroy_vms(vms_in_pool)

        # get resource pool instance
        pi = self.get_pool(pool, dc, cluster)
        WaitForTask(pi.Destroy())
        logger.info(f"Successfully deleted resource pool {pool}")

    def get_host(self, vm):
        """
        Fetches the Host for the VM. Host where VM resides

        Args:
            vm (vim.VirtualMachine): VM instance

        Returns:
             vim.HostSystem: Host instance

        """
        return vm.runtime.host

    def get_all_vms_in_dc(self, dc):
        """
        Fetches all VMs in Datacenter

        Args:
            dc (str): Datacenter name

        Returns:
            list: List of VMs instance in a Datacenter

        """
        vms = []
        dc = self.get_dc(dc)
        vmfolder = dc.vmFolder
        vmlist = vmfolder.childEntity
        for each in vmlist:
            if hasattr(each, "childEntity"):
                for vm in each.childEntity:
                    vms.append(vm)
            else:
                # Direct VMs created in cluster
                # This are the VMs created directly on cluster
                # without ResourcePool
                vms.append(each)
        return vms

    def get_host_obj(self, host_name):
        """
        Fetches the Host object

        Args:
            host_name (str): Host name

        Returns:
            vim.HostSystem: Host instance

        """
        content = self.get_content
        host_view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.HostSystem], True
        )
        host_obj = [host for host in host_view.view]
        host_view.Destroy()
        for host in host_obj:
            if host.name == host_name:
                return host

    def get_active_partition_from_mount_info(self, host):
        """
        Gets the active partition from mount info

        Args:
            host (vim.HostSystem): Host instance

        Returns:
            str: Active partition disk

        """
        logger.debug("Fetching active partition from fileSystemVolume information")
        mount_info = host.config.fileSystemVolume.mountInfo
        for each in mount_info:
            try:
                if each.volume.extent:
                    return each.volume.extent[0].diskName
            except AttributeError:
                continue

    def find_datastore_by_name(self, datastore_name, datacenter_name):
        """
        Fetches the Datastore

        Args:
            datastore_name (str): Name of the Datastore
            datacenter_name (str): Name of the Datacenter

        Returns:
            vim.Datastore: Datastore instance

        """
        dc = self.find_datacenter_by_name(datacenter_name)
        for ds in dc.datastore:
            if ds.name == datastore_name:
                return ds

    def find_datacenter_by_name(self, datacenter_name):
        """
        Fetches the Datacenter

        Args:
            datacenter_name (str): Name of the Datacenter

        Returns:
            vim.Datacenter: Datacenter instance

        """
        return self.find_object_by_name(
            self.get_content, datacenter_name, [vim.Datacenter]
        )

    def get_datastore_type(self, datastore):
        """
        Gets the Datastore Type

        Args:
            datastore (vim.Datastore): Datastore instance

        Returns:
            str: Datastore type. Either VMFS or vsan

        """
        return datastore.summary.type

    def get_datastore_type_by_name(self, datastore_name, datacenter_name):
        """
        Gets the Datastore Type

        Args:
            datastore_name (str): Name of the Datastore
            datacenter_name (str): Name of the Datacenter

        Returns:
            str: Datastore type. Either VMFS or vsan

        """
        datastore = self.find_datastore_by_name(datastore_name, datacenter_name)
        return self.get_datastore_type(datastore)

    def wait_for_task(self, task):
        """
        Wait for a task to finish

        Args:
            task (instance): Instance for the task

        Returns:
            instance: VM instance

        """
        task_done = False
        while not task_done:
            if task.info.state == "success":
                logger.debug("Cloning VM completed successfully")
                return task.info.result

            if task.info.state == "error":
                logger.error(f"Error while cloning the VM : {task.info.error.msg}")
                task_done = True

    def find_resource_pool_by_name(self, resource_pool_name):
        """
        Fetches the Resource Pool

        Args:
            resource_pool_name (str): Name of the Resource Pool

        Returns:
            instance: Resource Pool instance

        """
        return self.find_object_by_name(
            self.get_content, resource_pool_name, [vim.ResourcePool]
        )

    def get_compute_vms_in_pool(self, name, dc, cluster):
        """
        Gets all compute VM's in Resource pool

        Args:
            name (str): Resource pool name
            dc (str): Datacenter name
            cluster (str): Cluster name

        Returns:
            list: VM instances (vim.VirtualMachine)

        """
        vms = self.get_all_vms_in_pool(name, dc, cluster)
        return [vm for vm in vms if vm.name.startswith("compute")]
