/**
 * Copyright (C) 2012-2013 Dell, Inc.
 * See annotations for authorship information
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.virtustream.compute;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.compute.AbstractVMSupport;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineCapabilities;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VMFilterOptions;
import org.dasein.cloud.compute.VMLaunchOptions;
import org.dasein.cloud.compute.VMScalingCapabilities;
import org.dasein.cloud.compute.VMScalingOptions;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.network.RawAddress;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.virtustream.Virtustream;
import org.dasein.cloud.virtustream.VirtustreamMethod;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Kilobyte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;

public class VirtualMachines extends AbstractVMSupport {
    static private final Logger logger = Logger.getLogger(VirtualMachines.class);

    static private final String ALTER_VM                    =   "VM.alterVM";
    static private final String CLONE_VM                    =   "VM.cloneVm";
    static private final String GET_VIRTUAL_MACHINE         =   "VM.getVirtualMachine";
    static private final String IS_SUBSCRIBED               =   "VM.isSubscribed";
    static private final String LAUNCH_VM                   =   "VM.launchVM";
    static private final String LIST_VIRTUAL_MACHINES       =   "VM.listVms";
    static private final String LIST_VIRTUAL_MACHINE_STATUS =   "VM.listVmStatus";
    static private final String REBOOT_VIRTUAL_MACHINE      =   "VM.rebootVM";
    static private final String RESUME_VIRTUAL_MACHINE      =   "VM.resumeVM";
    static private final String START_VIRTUAL_MACHINE       =   "VM.startVM";
    static private final String STOP_VIRTUAL_MACHINE        =   "VM.stopVM";
    static private final String SUSPEND_VIRTUAL_MACHINE     =   "VM.suspendVM";
    static private final String TERMINATE_VM                =   "VM.terminateVM";

    static private final String FIND_RESOURCE_POOL          =   "VM.findResourcePool";
    static private final String FIND_STORAGE                =   "VM.findStorage";

    private Virtustream provider = null;

    public VirtualMachines(Virtustream provider) {
        super(provider);
        this.provider = provider;
    }

    @Override
    public VirtualMachine alterVirtualMachine(@Nonnull String vmId, @Nonnull VMScalingOptions options) throws InternalException, CloudException {
        APITrace.begin(provider, ALTER_VM);
        try {
            VirtualMachine vm = getVirtualMachine(vmId);
            if (vm == null) {
                throw new InternalException("Vm with id "+vmId+" does not exist.");
            }
            String body = options.getProviderProductId();
            String[] parts = body.split("\\[");
            VirtustreamMethod method = new VirtustreamMethod(provider);

            String productId = parts[0];
            VirtualMachineProduct vmProduct = getProduct(productId);
            int cpuCore = vmProduct.getCpuCount();
            Storage<Megabyte> ramSize = vmProduct.getRamSize();
            long ramAllocated = ramSize.longValue();

            if (!vm.getProductId().equals(productId)) {
                //Reconfigure VM
                //may need to stop vm first
                VmState state = vm.getCurrentState();
                boolean restart = false;
                if (!state.equals(VmState.STOPPED)) {
                    restart = true;
                    stop(vmId, true);
                    vm = getVirtualMachine(vmId);
                    while (!vm.getCurrentState().equals(VmState.STOPPED)) {
                        try {
                            Thread.sleep(15000L);
                        }
                        catch (InterruptedException ignore) {}
                        vm = getVirtualMachine(vmId);
                    }
                }

                JSONObject json = new JSONObject();
                try {
                    // create json request
                    json.put("VirtualMachineID", vmId);
                    json.put("NumCpu", cpuCore);
                    json.put("RamAllocatedMB", ramAllocated);
                    json.put("ResourcePoolID", vm.getTag("ResourcePoolID"));
                }
                catch (JSONException e) {
                    logger.error(e);
                    throw new InternalException("Unable to parse JSON "+e.getMessage());
                }

                String obj = method.postString("/VirtualMachine/ReconfigureVM", json.toString(), ALTER_VM);
                if (obj != null && obj.length() > 0) {
                    try {
                        json = new JSONObject(obj);
                        if (provider.parseTaskID(json) == null) {
                            logger.warn("No confirmation of ReconfigureVM task completion but no error either");
                        }
                    }
                    catch (JSONException e) {
                        logger.error(e);
                        throw new InternalException("Unable to parse JSON "+e.getMessage());
                    }
                }
                if (restart) {
                    start(vmId);
                }
            }

            if (parts.length > 1) {
                String dataCenterID = vm.getProviderDataCenterId();
                DataCenter dc = provider.getDataCenterServices().getDataCenter(dataCenterID);

                String volumeString = parts[1].substring(0, parts[1].length()-1);
                String[] volumes = volumeString.split(",");
                for (String v : volumes) {
                    String[] details = v.split(":");
                    if (details.length==2 ) {
                        String volumeId = details[0];
                        String sizeinGB = details[1];
                        Storage<Gigabyte> size = new Storage<Gigabyte>(Integer.parseInt(sizeinGB), Storage.GIGABYTE);
                        Storage<Kilobyte> capacity = (Storage<Kilobyte>)size.convertTo(Storage.KILOBYTE);
                        long capacityKB = capacity.longValue();

                        //find a suitable storage location for the hard disk
                        String storageId = findAvailableStorage(capacityKB, dc);
                        if (storageId == null) {
                            logger.error("No available storage resource in datacenter "+dc.getName());
                            throw new CloudException("No available storage resource in datacenter "+dc.getName());
                        }

                        if (volumeId.equals("new")) {
                            //add new disk
                            JSONObject disk = new JSONObject();
                            try {
                                disk.put("StorageID", storageId);
                                disk.put("CapacityKB", capacityKB);
                                disk.put("VirtualMachineID", vmId);
                            }
                            catch (JSONException e) {
                                logger.error(e);
                                throw new InternalException("Unable to parse JSON "+e.getMessage());
                            }
                            String obj = method.postString("VirtualMachine/AddDisk", disk.toString(), ALTER_VM);
                            if (obj != null && obj.length() > 0) {
                                try {
                                    JSONObject response = new JSONObject(obj);
                                    if (provider.parseTaskID(response) == null) {
                                        logger.warn("No confirmation of AddDisk task completion but no error either");
                                    }
                                }
                                catch (JSONException e) {
                                    logger.error(e);
                                    throw new InternalException("Unable to parse JSON "+e.getMessage());
                                }
                            }
                        }
                        else {
                            //reconfigure disk
                            JSONObject disk = new JSONObject();
                            try {
                                disk.put("VirtualMachineDiskID", volumeId);
                                disk.put("StorageID", storageId);
                                disk.put("CapacityKB", capacityKB);
                                disk.put("VirtualMachineID", vmId);
                            }
                            catch (JSONException e) {
                                logger.error(e);
                                throw new InternalException("Unable to parse JSON "+e.getMessage());
                            }
                            String obj = method.postString("VirtualMachine/ReconfigureDisk", disk.toString(), ALTER_VM);
                            if (obj != null && obj.length() > 0) {
                                try {
                                    JSONObject response = new JSONObject(obj);
                                    if (provider.parseTaskID(response) == null) {
                                        logger.warn("No confirmation of ReconfigureDisk task completion but no error either");
                                    }
                                }
                                catch (JSONException e) {
                                    logger.error(e);
                                    throw new InternalException("Unable to parse JSON "+e.getMessage());
                                }
                            }
                        }
                    }
                    else {
                        throw new InternalException("Volume string must take format <id>:<sizeInGB>. NB id is 'new' if adding new disk");
                    }
                }
            }

            return getVirtualMachine(vmId);
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public VirtualMachine clone(@Nonnull String vmId, @Nonnull String intoDcId, @Nonnull String name, @Nonnull String description, boolean powerOn, @Nullable String... firewallIds) throws InternalException, CloudException {
        APITrace.begin(provider, CLONE_VM);
        try {
            VirtustreamMethod method = new VirtustreamMethod(provider);
            String body;
            JSONObject json = new JSONObject();
            try {
                json.put("VirtualMachineID", vmId);
                json.put("Name", name);
                json.put("PowerOn", powerOn);
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse JSON "+json);
            }

            body = json.toString();
            String obj = method.postString("/VirtualMachine/CloneVM", body, CLONE_VM);

            String newVMId = null;
            if (obj != null && obj.length() > 0) {
                try {
                    JSONObject node = new JSONObject(obj);
                    newVMId = provider.parseTaskID(node);
                }
                catch (JSONException e) {
                    logger.error(e);
                    throw new InternalException("Unable to parse JSON "+e.getMessage());
                }
            }
            if (newVMId == null) {
                logger.error("Vm was cloned without error but new id not returned");
                throw new CloudException("Vm was cloned without error but new id not returned");
            }
            return getVirtualMachine(newVMId);
        }
        finally {
            APITrace.end();
        }
    }

    private transient volatile VMCapabilities capabilities;
    @Nonnull
    @Override
    public VirtualMachineCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new VMCapabilities(provider);
        }
        return capabilities;
    }

    @Nullable
    @Override
    public VirtualMachineProduct getProduct(@Nonnull String productId) throws InternalException, CloudException {
        String[] parts = productId.split(":");
        int cpuCount, ramInMb;

        if (parts.length < 2) {
            return null;
        }
        try {
            ramInMb = Integer.parseInt(parts[0]);
            cpuCount = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            return null;
        }
        VirtualMachineProduct product = new VirtualMachineProduct();

        product.setProviderProductId(productId);
        product.setName(ramInMb + "MB - " + cpuCount + " core" + (cpuCount > 1 ? "s" : ""));
        product.setRamSize(new Storage<Megabyte>(ramInMb, Storage.MEGABYTE));
        product.setCpuCount(cpuCount);
        product.setDescription(product.getName());
        product.setRootVolumeSize(new Storage<Gigabyte>(20, Storage.GIGABYTE));
        return product;
    }

    @Nullable
    @Override
    public VirtualMachine getVirtualMachine(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(provider, GET_VIRTUAL_MACHINE);
        try {
            VirtustreamMethod method = new VirtustreamMethod(provider);
            String obj = method.getString("/VirtualMachine/"+vmId+"?$filter=IsRemoved eq false", GET_VIRTUAL_MACHINE);

            if (obj != null && obj.length() > 0) {
                try {
                    JSONObject json = new JSONObject(obj);
                    VirtualMachine vm = toVirtualMachine(json);

                    if (vm != null) {
                        return vm;

                    }
                }
                catch (JSONException e) {
                    logger.error(e);
                    throw new InternalException("Unable to parse JSON "+e.getMessage());
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, IS_SUBSCRIBED);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                method.getString("/VirtualMachine?$filter=IsTemplate eq false and IsRemoved eq false", IS_SUBSCRIBED);
                return true;
            }
            catch (Throwable ignore) {
                return false;
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public VirtualMachine launch(@Nonnull VMLaunchOptions withLaunchOptions) throws CloudException, InternalException {
        APITrace.begin(provider, LAUNCH_VM);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                String body;

                String templateId = withLaunchOptions.getMachineImageId();
                String name = withLaunchOptions.getHostName();
                String description = withLaunchOptions.getDescription();
                String networkId = withLaunchOptions.getVlanId();
                if (networkId == null) {
                    logger.error("Network is mandatory when launching vms in virtustream");
                    throw new InternalException("Network is mandatory when launching vms in virtustream");
                }
                String tenantId = getContext().getAccountNumber();
                String dataCenterID = withLaunchOptions.getDataCenterId();
                DataCenter dc = provider.getDataCenterServices().getDataCenter(dataCenterID);

                long capacityKB;
                /*if (withLaunchOptions.getRootVolumeProductId() != null) {
                    String volumeProductId = withLaunchOptions.getRootVolumeProductId();
                    Volume volSupport = provider.getComputeServices().getVolumeSupport();
                    VolumeProduct volumeProduct = volSupport.getVolumeProduct(volumeProductId);
                    Storage<Gigabyte> size = volumeProduct.getVolumeSize();
                    Storage<Kilobyte> capacity = (Storage<Kilobyte>)size.convertTo(Storage.KILOBYTE);
                    capacityKB = capacity.longValue();
                }
                else {
                    capacityKB = 20971520;
                }  */
                capacityKB = 20971520;
                //get the device key for the template
                MachineImage img = provider.getComputeServices().getImageSupport().getImage(templateId);
                int deviceKey = Integer.parseInt(img.getTag("DeviceKey").toString());
                String ostype = (img.getPlatform().equals(Platform.WINDOWS)) ? "Windows" : "Linux";

                int cpuCore;
                long ramAllocated;
                if (withLaunchOptions.getStandardProductId() != null) {
                    String vmProductID = withLaunchOptions.getStandardProductId();
                    VirtualMachineProduct vmProduct = getProduct(vmProductID);
                    cpuCore = vmProduct.getCpuCount();
                    Storage<Megabyte> ramSize = vmProduct.getRamSize();
                    ramAllocated = ramSize.longValue();
                }
                else {
                    cpuCore = 1;
                    ramAllocated = 2048;
                }

                //find a suitable storage location for the hard disk
                String storageId = findAvailableStorage(capacityKB, dc);
                if (storageId == null) {
                    logger.error("No available storage resource in datacenter "+dc.getName());
                    throw new CloudException("No available storage resource in datacenter "+dc.getName());
                }
                JSONObject disk = new JSONObject();
                disk.put("StorageID", storageId);
                disk.put("CapacityKB", capacityKB);
                disk.put("DeviceKey", deviceKey);
                JSONArray disks = new JSONArray();
                disks.put(disk);

                JSONObject nic = new JSONObject();
                nic.put("NetworkID", networkId);
                nic.put("AdapterType", 1);
                JSONArray nics = new JSONArray();
                nics.put(nic);

                // get suitable resource pool
                String resourcePoolId = findAvailableResourcePool(dc);
                if (resourcePoolId == null) {
                    logger.error("No available resource pool in datacenter "+dc.getName());
                    throw new CloudException("No available resource pool in datacenter "+dc.getName());
                }

                //customisation of password and ip address and a whole bunch of mandatory params
               /* JSONObject customization = new JSONObject();
                String password = generatePassword();
                ProviderContext ctx = getContext();
                Properties prop = ctx.getCustomProperties();
                String timezoneLocation =  prop.getProperty("TimeZoneID");
                if (timezoneLocation == null) {
                    timezoneLocation = TimeZone.getDefault().getID();
                }

                if (ostype.equalsIgnoreCase("windows")) {
                    JSONArray networksArray = new JSONArray();
                    JSONObject networkCustom = new JSONObject();
                    networkCustom.put("NicNumber", 1);
                    networkCustom.put("IpAddressMode", 1); //dhcp
                    networksArray.put(networkCustom);

                    customization.put("AdministratorPassword", password);
                    customization.put("UseCustomNetworkSettings", "true");
                    customization.put("NetworkCustomizations", networksArray);
                    customization.put("GuestOsType", ostype);
                    customization.put("GuestOsOwnerName", "Owner");
                    customization.put("GuestOsOwnerOrganization", "Org") ;
                    customization.put("DomainName", "Virtustream");
                    customization.put("TimeZone", timezoneLocation);
                    customization.put("DomainAdminUsername", "Administrator");
                    customization.put("DomainAdminPassword", password);
                    customization.put("ComputerNameOption", 3);  //use vm name
                    //todo this seems to be necessary to get windows servers working from the template they provide
                    //but doing this in such a hidden way to the user is a bad idea
                    //check what we should really do but to let windows testing occur lets keep it in for now
                    customization.put("GenerateNewSid", "true");
                }
                else {
                    JSONArray networksArray = new JSONArray();
                    JSONObject networkCustom = new JSONObject();
                    networkCustom.put("NicNumber", 1);
                    networkCustom.put("IpAddressMode", 1); //dhcp
                    networksArray.put(networkCustom);

                    JSONArray dnsSearchPaths = new JSONArray();
                    dnsSearchPaths.put("xstream.local");

                    customization.put("UseCustomNetworkSettings", "true");
                    customization.put("NetworkCustomizations", networksArray);
                    customization.put("GuestOsType", ostype);
                    customization.put("DomainName", "Virtustream");
                    customization.put("DnsSearchPaths", dnsSearchPaths);
                    customization.put("TimeZoneLocation", timezoneLocation);
                    customization.put("ComputerNameOption", 3);  //use vm name
                }  */

                //***************************************************

                // create json request
                JSONObject vmJson = new JSONObject();
                vmJson.put("Description", description);
                vmJson.put("Disks", disks);
                vmJson.put("Nics", nics);
                vmJson.put("NumCpu", cpuCore);
                vmJson.put("RamAllocatedMB", ramAllocated);
                vmJson.put("ResourcePoolID", resourcePoolId);
                vmJson.put("SourceTemplateID", templateId);
                vmJson.put("TenantID", tenantId);
                vmJson.put("CustomerDefinedName", name);
              //  vmJson.put("CustomizationSpecification", customization);

                body = vmJson.toString();

                String obj = method.postString("/VirtualMachine/SetVM", body, LAUNCH_VM);
                if (obj != null && obj.length() > 0) {
                    JSONObject json = new JSONObject(obj);
                    String vmId = provider.parseTaskID(json);
                    if (vmId != null) {
                        VirtualMachine vm = getVirtualMachine(vmId);
                     //   vm.setRootPassword(password);
                        return vm;
                    }
                }
                logger.error("Vm was launched without error but new id not returned");
                throw new CloudException("Vm was launched without error but new id not returned");
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse JSONObject "+e.getMessage());
            }
        }
        finally {
            APITrace.end();
        }
    }

    static private final Random random = new Random();
    private @Nonnull String generatePassword() {
        int len = 8 + random.nextInt(5);
        StringBuilder password = new StringBuilder();

        while( password.length() < len ) {
            char c = (char)random.nextInt(255);

            if( (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ) {
                if( c != 'I' && c != 'i' && c != 'o' && c != 'O' && c != 'l' ) {
                    password.append(c);
                }
            }
            else if( c >= '2' && c <='9' ) {
                password.append(c);
            }
            else if( c == '%' || c == '@' || c == '#' || c == '$' || c == '[' || c == ']' ) {
                password.append(c);
            }
        }
        return password.toString();
    }

    private transient ArrayList<VirtualMachineProduct> cachedProducts;

    @Nonnull
    @Override
    public Iterable<VirtualMachineProduct> listProducts(@Nonnull Architecture architecture) throws InternalException, CloudException {
        ArrayList<VirtualMachineProduct> products = cachedProducts;

        if (products == null) {
            products = new ArrayList<VirtualMachineProduct>();

            for (int ram : new int[]{1024, 2048, 4096, 8192, 12288, 16384, 20480, 24576, 28668, 32768}) {
                for (int cpuCount : new int[]{1, 2, 3, 4, 5, 6, 7, 8}) {
                    products.add(getProduct(ram + ":" + cpuCount));
                }
            }
            cachedProducts = products;
        }
        return products;
    }

    @Nonnull
    @Override
    public Iterable<ResourceStatus> listVirtualMachineStatus() throws InternalException, CloudException {
        APITrace.begin(provider, LIST_VIRTUAL_MACHINE_STATUS);
        try {
            try {
                ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
                VirtustreamMethod method = new VirtustreamMethod(provider);
                String obj = method.getString("/VirtualMachine?$filter=IsTemplate eq false and IsRemoved eq false", LIST_VIRTUAL_MACHINE_STATUS);

                if (obj != null && obj.length() > 0) {
                    JSONArray json = new JSONArray(obj);
                    for (int i = 0; i<json.length(); i++) {
                        JSONObject node = json.getJSONObject(i);
                        ResourceStatus status = toStatus(node);

                        if (status != null) {
                            list.add(status);
                        }
                    }
                }
                return list;
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse JSONObject "+e.getMessage());
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        return listVirtualMachines(null);
    }

    @Nonnull
    @Override
    public Iterable<VirtualMachine> listVirtualMachines(@Nullable VMFilterOptions options) throws InternalException, CloudException {
        APITrace.begin(provider, LIST_VIRTUAL_MACHINES);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                ArrayList<VirtualMachine> list = new ArrayList<VirtualMachine>();
                String obj = method.getString("/VirtualMachine?$filter=IsTemplate eq false and IsRemoved eq false", LIST_VIRTUAL_MACHINES);

                if (obj != null && obj.length() > 0) {
                    JSONArray json = new JSONArray(obj);
                    for (int i= 0; i<json.length(); i++) {
                        VirtualMachine vm = toVirtualMachine(json.getJSONObject(i));

                        if (vm != null && (options == null || options.matches(vm))) {
                            list.add(vm);
                        }
                    }
                }
                return list;
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse JSONObject "+e.getMessage());
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void reboot(@Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(provider, REBOOT_VIRTUAL_MACHINE);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                String obj = method.postString("/VirtualMachine/"+vmId+"/RebootOS", "", REBOOT_VIRTUAL_MACHINE);
                if (obj != null && obj.length()> 0) {
                    JSONObject json = new JSONObject(obj);
                    if (provider.parseTaskID(json) == null) {
                        logger.warn("No confirmation of RebootVM task completion but no error either");
                    }
                }
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse JSONObject "+e.getMessage());
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void resume(@Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(provider, RESUME_VIRTUAL_MACHINE);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                String obj = method.postString("/VirtualMachine/"+vmId+"/PowerOn", "", RESUME_VIRTUAL_MACHINE);
                if (obj != null && obj.length()> 0) {
                    JSONObject json = new JSONObject(obj);
                    if (provider.parseTaskID(json) == null) {
                        logger.warn("No confirmation of ResumeVM task completion but no error either");
                    }
                }
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse JSONObject "+e.getMessage());
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void start(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(provider, START_VIRTUAL_MACHINE);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                String obj = method.postString("/VirtualMachine/"+vmId+"/PowerOn", "", START_VIRTUAL_MACHINE);
                if (obj != null && obj.length()> 0) {
                    JSONObject json = new JSONObject(obj);
                    if (provider.parseTaskID(json) == null) {
                        logger.warn("No confirmation of StartVM task completion but no error either");
                    }
                }
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse JSONObject "+e.getMessage());
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void stop(@Nonnull String vmId, boolean force) throws InternalException, CloudException {
        APITrace.begin(provider, STOP_VIRTUAL_MACHINE);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                if (force) {
                    String obj = method.postString("/VirtualMachine/"+vmId+"/PowerOff", "", STOP_VIRTUAL_MACHINE);
                    if (obj != null && obj.length()> 0) {
                        JSONObject json = new JSONObject(obj);
                        if (provider.parseTaskID(json) == null) {
                            logger.warn("No confirmation of StopVM task completion but no error either");
                        }
                    }
                }
                else {
                    String obj = method.postString("/VirtualMachine/"+vmId+"/ShutdownOS", "", STOP_VIRTUAL_MACHINE);
                    if (obj != null && obj.length() > 0) {
                        JSONObject json = new JSONObject(obj);
                        try {
                            if (provider.parseTaskID(json) == null) {
                                logger.warn("No confirmation of ShutdownOS task completion but no error either");
                            }
                        }
                        catch (CloudException ignore) {
                            logger.error("Unable to shutdown os: "+ignore.getMessage()+" trying force stop");
                            stop(vmId, true);
                        }
                    }
                }
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse JSONObject "+e.getMessage());
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void suspend(@Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(provider, SUSPEND_VIRTUAL_MACHINE);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                String obj = method.postString("/VirtualMachine/"+vmId+"/Suspend", "", SUSPEND_VIRTUAL_MACHINE);
                if (obj != null && obj.length()> 0) {
                    JSONObject json = new JSONObject(obj);
                    if (provider.parseTaskID(json) == null) {
                        logger.warn("No confirmation of SuspendVM task completion but no error either");
                    }
                }
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse JSONObject "+e.getMessage());
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void terminate(@Nonnull String vmId) throws CloudException, InternalException {
        terminate(vmId, "");
    }

    @Override
    public void terminate(@Nonnull String vmId, @Nullable String explanation) throws InternalException, CloudException {
        APITrace.begin(provider, TERMINATE_VM);
        try {
            VirtustreamMethod method = new VirtustreamMethod(provider);
            VirtualMachine vm = getVirtualMachine(vmId);
            if (!vm.getCurrentState().equals(VmState.STOPPED)) {
                stop(vmId, true);
                vm = getVirtualMachine(vmId);
                long timeout = System.currentTimeMillis()+(CalendarWrapper.MINUTE * 5);
                while (timeout > System.currentTimeMillis()) {
                    try {
                        Thread.sleep(15000L);
                    }
                    catch (InterruptedException ignore) {}
                    vm = getVirtualMachine(vmId);
                    if (vm.getCurrentState().equals(VmState.STOPPED)) {
                        break;
                    }
                }
            }
            if (vm.getCurrentState().equals(VmState.STOPPED)) {
                String obj = method.postString("/VirtualMachine/"+vmId+"/Remove", "", TERMINATE_VM);
                if (obj != null && obj.length() > 0) {
                    try {
                        JSONObject json = new JSONObject(obj);
                        if (provider.parseTaskID(json) == null) {
                            logger.warn("No confirmation of TerminateVM task completion but no error either");
                        }
                    }
                    catch (JSONException e) {
                        logger.error(e);
                        throw new InternalException("Unable to parse JSONObject "+e.getMessage());
                    }

                }
            }
            else {
                logger.error("Server not stopping so can't be deleted");
                throw new CloudException("Server not stopping so can't be deleted");
            }
        }
        finally {
            APITrace.end();
        }
    }

    private VirtualMachine toVirtualMachine(@Nonnull JSONObject json) throws InternalException, CloudException {
        try {
            VirtualMachine vm = new VirtualMachine();
            vm.setClonable(false);
            vm.setImagable(false);
            vm.setPausable(true);
            vm.setPersistent(true);
            vm.setRebootable(true);

            String id;
            if (!json.isNull("VirtualMachineID"))  {
                id = json.getString("VirtualMachineID");
                vm.setProviderVirtualMachineId(id);
            }
            else {
                return null;
            }

            if (json.has("CustomerDefinedName") && !json.isNull("CustomerDefinedName")) {
                vm.setName(json.getString("CustomerDefinedName"));
            }

            //check this is indeed a vm
            boolean isTemplate = json.getBoolean("IsTemplate");
            if (isTemplate) {
                logger.error("Resource with id "+id+" is a template");
                return null;
            }

            boolean isRemoved = json.getBoolean("IsRemoved");
            if (isRemoved) {
                logger.debug("IsRemoved flag is set so not returning vm "+vm.getProviderVirtualMachineId());
                return null;
            }

            if (json.has("Description") && !json.isNull("Description")) {
                vm.setDescription(json.getString("Description"));
            }

            vm.setPlatform(Platform.guess(json.getString("OS")));
            Architecture arch = guess(json.getString("OSFullName"));
            vm.setArchitecture(arch);

            if (json.has("TenantID") && !json.isNull("TenantID")) {
                vm.setProviderOwnerId(json.getString("TenantID"));
            }
            else {
                logger.warn("No tenant id found for "+id);
                return null;
            }
            String regionId = null;
            if (json.has("RegionID") && !json.isNull("RegionID")) {
                regionId = json.getString("RegionID");
            }

            if (json.has("Hypervisor") && !json.isNull("Hypervisor")) {
                JSONObject hv = json.getJSONObject("Hypervisor");
                JSONObject site = hv.getJSONObject("Site");
                vm.setProviderDataCenterId(site.getString("SiteID"));
                if (regionId == null || regionId.equals("0")) {
                    //get region from hypervisor site
                    JSONObject r = site.getJSONObject("Region");
                    regionId = r.getString("RegionID");
                }
            }

            if (json.has("BootTime") && !json.isNull("BootTime")) {
                String string_date = json.getString("BootTime");

                SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                try {
                    Date d = f.parse(string_date);
                    long milliseconds = d.getTime();
                    vm.setLastBootTimestamp(milliseconds);
                    vm.setCreationTimestamp(milliseconds);
                }
                catch (ParseException e) {
                    logger.error(e);
                }
            }

            if (json.has("PowerState") && !json.isNull("PowerState"))  {
                String state = json.getString("PowerState");
                if (state.equalsIgnoreCase("poweredoff")) {
                    vm.setCurrentState(VmState.STOPPED);
                    vm.setImagable(true);
                    vm.setClonable(true);
                }
                else if (state.equalsIgnoreCase("poweredon")) {
                    vm.setCurrentState(VmState.RUNNING);
                }
                else if (state.equalsIgnoreCase("suspended")) {
                    vm.setCurrentState(VmState.SUSPENDED);
                }
                else {
                    logger.warn("Unknown vm state "+state);
                }
            }

            if (json.has("Nics") && !json.isNull("Nics")) {
                JSONArray nics = json.getJSONArray("Nics");
                JSONObject nic = nics.getJSONObject(0);
                if (nic.has("NetworkID") && !nic.isNull("NetworkID")) {
                    vm.setProviderVlanId(nic.getString("NetworkID"));
                }
                if (nic.has("VirtualMachineNicID") && !nic.isNull("VirtualMachineNicID")) {
                    vm.setTag("VirtualMachineNicID", nic.getString("VirtualMachineNicID"));
                }
            }

            if (json.has("IPAddress") && !json.isNull("IPAddress")) {
                String addr = json.getString("IPAddress");
                boolean isPub = isPublicAddress(addr);
                if( isPub ) {
                    vm.setPublicAddresses(new RawAddress(addr));
                    if( vm.getPublicDnsAddress() == null ) {
                        vm.setPublicDnsAddress(addr);
                    }
                }
                else {
                    vm.setPrivateAddresses(new RawAddress(addr));
                    if( vm.getPrivateDnsAddress() == null ) {
                        vm.setPrivateDnsAddress(addr);
                    }
                }
            }

            String cpuCount, ramAllocatedMB;
            cpuCount = json.getString("NumCpu");
            ramAllocatedMB = json.getString("RamAllocatedMB");
            vm.setProductId(ramAllocatedMB + ":" + cpuCount);

            String resourcePoolID = json.getString("ResourcePoolID");
            vm.setTag("ResourcePoolID", resourcePoolID);

            if (regionId == null) {
                logger.warn("Unable to find region id for virtual machine "+id);
                regionId = getContext().getRegionId();
            }

            vm.setProviderRegionId(regionId);

            if (vm.getName() == null) {
                vm.setName(vm.getProviderVirtualMachineId());
            }
            if (vm.getDescription() == null) {
                vm.setDescription(vm.getName());
            }

            return vm;
        }
        catch (JSONException e) {
            logger.error(e);
            throw  new InternalException("Unable to parse JSONObject "+e.getMessage());
        }
    }

    private ResourceStatus toStatus(@Nonnull JSONObject node) throws InternalException, CloudException {
        try {
            String id = node.getString("VirtualMachineID");
            boolean isTemplate = node.getBoolean("IsTemplate");

            if (id == null || isTemplate) {
                return null;
            }
            String state;
            VmState vmState = null;
            if (node.has("PowerState") && !node.isNull("PowerState")) {
                state = node.getString("PowerState");
                if (state.equalsIgnoreCase("poweredoff")) {
                    vmState = VmState.STOPPED;
                }
                else if (state.equalsIgnoreCase("poweredon")) {
                    vmState = VmState.RUNNING;
                }
                else if (state.equalsIgnoreCase("suspended")) {
                    vmState = VmState.SUSPENDED;
                }
                else {
                    logger.warn("Unknown state "+state);
                }
            }
            if (vmState != null) {
                return new ResourceStatus(id, vmState);
            }
            return null;
        }
        catch (JSONException e) {
            logger.error(e);
            throw new InternalException("Unable to parse JSONObject "+e.getMessage());
        }
    }

    private Architecture guess(String desc) {
        Architecture arch = Architecture.I64;

        if( desc.contains("x64") ) {
            arch = Architecture.I64;
        }
        else if( desc.contains("x32") ) {
            arch = Architecture.I32;
        }
        else if( desc.contains("64 bit") ) {
            arch = Architecture.I64;
        }
        else if( desc.contains("32 bit") ) {
            arch = Architecture.I32;
        }
        else if( desc.contains("i386") ) {
            arch = Architecture.I32;
        }
        else if( desc.contains("64") ) {
            arch = Architecture.I64;
        }
        else if( desc.contains("32") ) {
            arch = Architecture.I32;
        }
        return arch;
    }

    public String findAvailableStorage(@Nonnull long capacityKB, @Nonnull DataCenter dataCenter) throws CloudException, InternalException {
        APITrace.begin(provider, FIND_STORAGE);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                HashMap<String, Integer> map = new HashMap<String, Integer>();
                String obj = method.getString("/Storage?$filter=IsRemoved eq false and Hypervisor/Site/SiteID eq '"+dataCenter.getProviderDataCenterId()+"'", FIND_STORAGE);
                if (obj != null && obj.length() > 0) {
                    JSONArray list = new JSONArray(obj);
                    for (int i=0; i<list.length(); i++) {
                        JSONObject json = list.getJSONObject(i);
                        String id = json.getString("StorageID");
                        long freeSpaceKB = json.getLong("FreeSpaceKB");
                        long storageCapacityKB = json.getLong("CapacityKB");
                        int percentFree = Math.round((storageCapacityKB/freeSpaceKB)*100);

                        // as long as the storage has enough free space we can use it
                        if (capacityKB <= freeSpaceKB) {
                            map.put(id, percentFree);
                        }
                    }
                }
                if (map.isEmpty()) {
                    logger.error("No available storage in datacenter "+dataCenter.getName()+" - require "+capacityKB+"KB");
                    throw new CloudException("No available storage in datacenter "+dataCenter.getName()+" - require "+capacityKB+"KB");
                }
                if (map.size() == 1) {
                    return map.keySet().iterator().next();
                }

                // return storage with least amount of free space
                Map.Entry<String, Integer> maxEntry = null;

                for (Map.Entry<String, Integer> entry : map.entrySet()) {
                    if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0) {
                        maxEntry = entry;
                    }
                }
                return maxEntry.getKey();
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse JSONObject "+e.getMessage());
            }
        }
        finally {
            APITrace.end();
        }
    }

    public String findAvailableResourcePool(@Nonnull DataCenter dataCenter) throws InternalException, CloudException {
        APITrace.begin(provider, FIND_RESOURCE_POOL);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                String obj = method.getString("/ResourcePool?$filter=IsRemoved eq false and Hypervisor/Site/SiteID eq '"+dataCenter.getProviderDataCenterId()+"'", FIND_RESOURCE_POOL);

                String firstResourcePool = null;
                if (obj != null && obj.length() > 0) {
                    JSONArray list = new JSONArray(obj);
                    for (int i=0; i<list.length(); i++) {
                        JSONObject json = list.getJSONObject(i);

                        String id = json.getString("ResourcePoolID");
                        if (i == 0) {
                            firstResourcePool = id;
                        }
                        if (json.has("VirtualMachineIDs") && !json.isNull("VirtualMachineIDs")) {
                            // found a resource pool currently in use so let's use it too
                            return id;
                        }
                    }
                }
                if (firstResourcePool == null) {
                    logger.error("No available resource pool in datacenter "+dataCenter.getName());
                    throw new CloudException("No available resource pool in datacenter "+dataCenter.getName());
                }
                // just return first resource pool found if none are currently in use
                return firstResourcePool;
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse JSONObject "+e.getMessage());
            }
        }
        finally {
            APITrace.end();
        }
    }

    private boolean isPublicAddress(@Nonnull String addr) {
        if( !addr.startsWith("10.") && !addr.startsWith("192.168.") ) {
            if( addr.startsWith("172.") ) {
                String[] nums = addr.split("\\.");

                if( nums.length != 4 ) {
                    return true;
                }
                else {
                    try {
                        int x = Integer.parseInt(nums[1]);

                        if( x < 16 || x > 31 ) {
                            return true;
                        }
                    }
                    catch( NumberFormatException ignore ) {
                        // ignore
                    }
                }
            }
            else {
                return true;
            }
        }
        return false;
    }
}
