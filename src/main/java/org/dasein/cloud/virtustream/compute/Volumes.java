/**
 * Copyright (C) 2012-2015 Dell, Inc.
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
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.cloud.virtustream.Virtustream;
import org.dasein.cloud.virtustream.VirtustreamMethod;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Kilobyte;
import org.dasein.util.uom.storage.Storage;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

public class Volumes extends AbstractVolumeSupport {
    static private final Logger logger = Logger.getLogger(Volume.class);

    static private final String GET_VOLUMES         =   "Volume.getVolume";
    static private final String IS_SUBSCRIBED       =   "Volume.isSubscribed";
    static private final String LIST_VOLUMES        =   "Volume.listVolumes";
    static private final String LIST_VOLUME_STATUS  =   "Volume.listVolumeStatus";
    static private final String REMOVE_VOLUMES      =   "Volume.removeVolume";

    private Virtustream provider = null;

    public Volumes(@Nonnull Virtustream provider) {
        super(provider);
        this.provider = provider;
    }

    private transient volatile VSVolumeCapabilities capabilities;
    @Override
    public VolumeCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new VSVolumeCapabilities(provider);
        }
        return capabilities;
    }

    @Nonnull
    @Override
    public String createVolume(@Nonnull VolumeCreateOptions options) throws InternalException, CloudException {
        APITrace.begin(provider, "Volumes.createVolume");
        try {
            if (!options.getFormat().equals(VolumeFormat.BLOCK)) {
                throw new OperationNotSupportedException("Only block volume creation supported in "+provider.getCloudName());
            }
            if (options.getSnapshotId() != null) {
                throw new OperationNotSupportedException("Creating volumes from snapshots not supported in "+provider.getCloudName());
            }
            if (options.getProviderVirtualMachineId() == null) {
                throw new CloudException("Volumes can only be created in the context of vms in "+provider.getCloudName()+". VM is null");
            }
            VirtustreamMethod method = new VirtustreamMethod(provider);
            String vmId = options.getProviderVirtualMachineId();
            VirtualMachines support = provider.getComputeServices().getVirtualMachineSupport();
            VirtualMachine vm = support.getVirtualMachine(vmId);
            String dataCenterID = vm.getProviderDataCenterId();
            DataCenter dc = provider.getDataCenterServices().getDataCenter(dataCenterID);

            //get existing disks
            String vmObj = method.getString("/VirtualMachine/"+vmId+"?$filter=IsRemoved eq false", "Volume.getVirtualMachine");
            List<String> diskIds = new ArrayList<String>();
            if (vmObj != null && vmObj.length() > 0) {
                try {
                    JSONObject json = new JSONObject(vmObj);
                    JSONArray disks = json.getJSONArray("Disks");
                    for (int j=0; j<disks.length(); j++) {
                        JSONObject diskJson = disks.getJSONObject(j);

                        diskIds.add(diskJson.getString("VirtualMachineDiskID"));
                    }
                }
                catch (JSONException e) {
                    logger.error(e);
                    throw new InternalException("Unable to parse JSON "+e.getMessage());
                }
            }

            Storage<Gigabyte> size = options.getVolumeSize();
            Storage<Kilobyte> capacity = (Storage<Kilobyte>)size.convertTo(Storage.KILOBYTE);
            long capacityKB = capacity.longValue();

            //find a suitable storage location for the hard disk based on the vms resource pool id
            storageComputeId = getComputeIDFromResourcePool(vm.getTag("ResourcePoolID").toString());
            String storageId = findAvailableStorage(capacityKB, dc);
            if (storageId == null) {
                logger.error("No available storage resource in datacenter "+dc.getName());
                throw new CloudException("No available storage resource in datacenter "+dc.getName());
            }

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
            String obj = method.postString("VirtualMachine/AddDisk", disk.toString(), "Volume.createVolume");
            if (obj != null && obj.length() > 0) {
                try {
                    JSONObject response = new JSONObject(obj);
                    provider.parseTaskId(response);
                    vmObj = method.getString("/VirtualMachine/"+vmId+"?$filter=IsRemoved eq false", "Volume.getVirtualMachine");

                    if (vmObj != null && vmObj.length() > 0) {
                        try {
                            JSONObject json = new JSONObject(vmObj);
                            JSONArray disks = json.getJSONArray("Disks");
                            for (int j=0; j<disks.length(); j++) {
                                JSONObject diskJson = disks.getJSONObject(j);

                                String diskId = diskJson.getString("VirtualMachineDiskID");
                                if (!diskIds.contains(diskId)) {
                                    Volume vol = getVolume(diskId);
                                    if (vol != null) {
                                        return diskId;
                                    }
                                }
                            }
                        }
                        catch (JSONException e) {
                            logger.error(e);
                            throw new InternalException("Unable to parse JSON "+e.getMessage());
                        }
                    }
                }
                catch (JSONException e) {
                    logger.error(e);
                    throw new InternalException("Unable to parse JSON "+e.getMessage());
                }
            }
            throw new CloudException("Can't find new volume");
        }
        finally {
            APITrace.end();
        }
    }

    @Nullable
    @Override
    public Storage<Gigabyte> getMaximumVolumeSize() throws InternalException, CloudException {
        return new Storage<Gigabyte>(1024, Storage.GIGABYTE);
    }

    @Nonnull
    @Override
    public Storage<Gigabyte> getMinimumVolumeSize() throws InternalException, CloudException {
        return new Storage<Gigabyte>(1, Storage.GIGABYTE);
    }

    @Nonnull
    @Override
    public String getProviderTermForVolume(@Nonnull Locale locale) {
        return "Disk";
    }

    @Override
    public Volume getVolume(@Nonnull String volumeId) throws InternalException, CloudException {
        APITrace.begin(provider, GET_VOLUMES);
        try {
            return super.getVolume(volumeId);
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public Iterable<String> listPossibleDeviceIds(@Nonnull Platform platform) throws InternalException, CloudException {
        Cache<String> cache;

        if( platform.isWindows() ) {
            cache = Cache.getInstance(getProvider(), "windowsDeviceIds", String.class, CacheLevel.CLOUD);
        }
        else {
            cache = Cache.getInstance(getProvider(), "unixDeviceIds", String.class, CacheLevel.CLOUD);
        }
        Iterable<String> ids = cache.get(getContext());

        if( ids == null ) {
            ArrayList<String> list = new ArrayList<String>();

            if( platform.isWindows() ) {
                list.add("hda");
                list.add("hdb");
                list.add("hdc");
                list.add("hdd");
                list.add("hde");
                list.add("hdf");
                list.add("hdg");
                list.add("hdh");
                list.add("hdi");
                list.add("hdj");
            }
            else {
                list.add("/dev/sda");
                list.add("/dev/sdb");
                list.add("/dev/sdc");
                list.add("/dev/sdd");
                list.add("/dev/sde");
                list.add("/dev/sdf");
                list.add("/dev/sdg");
                list.add("/dev/sdh");
                list.add("/dev/sdi");
            }
            ids = Collections.unmodifiableList(list);
            cache.put(getContext(), ids);
        }
        return ids;
    }

    @Nonnull
    @Override
    public Iterable<ResourceStatus> listVolumeStatus() throws InternalException, CloudException {
        APITrace.begin(provider, LIST_VOLUME_STATUS);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();

                String obj = method.getString("/VirtualMachine?$filter=IsTemplate eq false and IsRemoved eq false", LIST_VOLUMES);
                if (obj != null && obj.length() > 0) {
                    JSONArray array = new JSONArray(obj);
                    for (int i=0; i<array.length(); i++) {
                        JSONObject json = array.getJSONObject(i);

                        if (json.isNull("Disks")) {
                            continue;
                        }
                        JSONArray disks = json.getJSONArray("Disks");
                        for (int j=0; j<disks.length(); j++) {
                            JSONObject diskJson = disks.getJSONObject(j);

                            String id = diskJson.getString("VirtualMachineDiskID");
                            ResourceStatus status = new ResourceStatus(id, VolumeState.AVAILABLE);
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
    public Iterable<Volume> listVolumes() throws InternalException, CloudException {
        return listVolumes(null);
    }

    transient String vmID = null;
    transient String dataCenterID = null;
    transient String regionID = null;
    transient Platform platfrom = null;
    @Nonnull
    @Override
    public Iterable<Volume> listVolumes(@Nullable VolumeFilterOptions options) throws InternalException, CloudException {
        APITrace.begin(provider, LIST_VOLUMES);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                ArrayList<Volume> list = new ArrayList<Volume>();

                String obj = method.getString("/VirtualMachine?$filter=IsTemplate eq false and IsRemoved eq false", LIST_VOLUMES);
                if (obj != null && obj.length() > 0) {
                    JSONArray array = new JSONArray(obj);
                    for (int i=0; i<array.length(); i++) {
                        JSONObject json = array.getJSONObject(i);

                        //parse out vm info
                        parseVMData(json);

                        if (json.isNull("Disks")) {
                            continue;
                        }
                        JSONArray disks = json.getJSONArray("Disks");
                        for (int j=0; j<disks.length(); j++) {
                            JSONObject diskJson = disks.getJSONObject(j);

                            // create Volume object
                            Volume volume = toVolume(vmID, platfrom, regionID, dataCenterID, diskJson);
                            if (volume != null && (options == null || options.matches(volume))) {
                                list.add(volume);
                            }
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
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, IS_SUBSCRIBED);
        try {
            VirtualMachines support = provider.getComputeServices().getVirtualMachineSupport();
            return support.isSubscribed();
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void remove(@Nonnull String volumeId) throws InternalException, CloudException {
        APITrace.begin(provider, REMOVE_VOLUMES);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                JSONObject json = new JSONObject();
                json.put("VirtualMachineDiskID", volumeId);

                Volume vol =  getVolume(volumeId);
                if (vol != null) {
                    String vmID = vol.getProviderVirtualMachineId();
                    json.put("VirtualMachineID", vmID);

                    String body = json.toString();
                    String obj = method.postString("/VirtualMachine/RemoveDisk", body, REMOVE_VOLUMES);
                    if (obj != null && obj.length() > 0) {
                        JSONObject response = new JSONObject(obj);
                        if (provider.parseTaskId(response) == null) {
                            logger.warn("No confirmation of RemoveVolume task completion but no error either");
                        }
                    }
                }
                else {
                    throw new CloudException("Cannot find volume with id "+volumeId);
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

    private void parseVMData(JSONObject json) throws InternalException, CloudException {
        try {
            vmID = json.getString("VirtualMachineID");
            if (json.has("RegionID") && !json.isNull("RegionID")) {
                regionID = json.getString("RegionID");
            }

            if (json.has("Hypervisor") && !json.isNull("Hypervisor")) {
                JSONObject hv = json.getJSONObject("Hypervisor");
                JSONObject site = hv.getJSONObject("Site");
                dataCenterID = site.getString("SiteID");
                if (regionID == null || regionID.equals("0")) {
                    //get region from hypervisor site
                    JSONObject r = site.getJSONObject("Region");
                    regionID = r.getString("RegionID");
                }
            }
            platfrom = Platform.guess(json.getString("OS"));
        }
        catch (JSONException e) {
            logger.error(e);
            throw new InternalException("Unable to parse JSONObject "+e.getMessage());
        }
    }

    private Volume toVolume(@Nonnull String vmID, @Nonnull Platform platform, @Nonnull String regionID, @Nonnull String dataCenterID, @Nonnull JSONObject json) throws InternalException, CloudException {
        try {
            Volume volume = new Volume();
            volume.setCurrentState(VolumeState.AVAILABLE);
            volume.setProviderDataCenterId(dataCenterID);
            volume.setProviderRegionId(regionID);
            volume.setProviderVirtualMachineId(vmID);
            volume.setFormat(VolumeFormat.BLOCK);
            volume.setType(VolumeType.HDD);

            volume.setProviderVolumeId(json.getString("VirtualMachineDiskID"));

            String diskName = json.getString("DiskFileName");
            diskName = diskName.substring(diskName.indexOf("/")+1);
            volume.setName(diskName);
            volume.setDescription(volume.getName());

            long diskSize = json.getLong("CapacityKB");
            Storage<Kilobyte> size = new Storage<Kilobyte>(diskSize, Storage.KILOBYTE);
            volume.setSize((Storage<Gigabyte>)size.convertTo(Storage.GIGABYTE));

            String deviceId = json.getString("UnitNumber");
            volume.setDeviceId(toDeviceID(deviceId, platform.equals(Platform.WINDOWS)));
            int diskNum = json.getInt("DiskNumber");
            if (diskNum == 0) {
                volume.setRootVolume(true);
                volume.setGuestOperatingSystem(platform);
            }
            return volume;
        }
        catch (JSONException e) {
            logger.error(e);
            throw new InternalException("Unable to parse JSONObject "+e.getMessage());
        }
    }

    private @Nonnull String toDeviceID(@Nonnull String deviceNumber, boolean isWindows) {
        if (deviceNumber == null){
            return null;
        }
        if (!isWindows){
            if( deviceNumber.equals("0") ) { return "/dev/sda"; }
            else if( deviceNumber.equals("1") ) { return "/dev/sdb"; }
            else if( deviceNumber.equals("2") ) { return "/dev/sdc"; }
            else if( deviceNumber.equals("3") ) { return "/dev/sdd"; }
            else if( deviceNumber.equals("4") ) { return "/dev/sde"; }
            else if( deviceNumber.equals("5") ) { return "/dev/sdf"; }
            else if( deviceNumber.equals("6") ) { return "/dev/sdg"; }
            else if( deviceNumber.equals("8") ) { return "/dev/sdh"; }
            else if( deviceNumber.equals("9") ) { return "/dev/sdi"; }
            else { return "/dev/sdi"; }
        }
        else{
            if( deviceNumber.equals("0") ) { return "hda"; }
            else if( deviceNumber.equals("1") ) { return "hdb"; }
            else if( deviceNumber.equals("2") ) { return "hdc"; }
            else if( deviceNumber.equals("3") ) { return "hdd"; }
            else if( deviceNumber.equals("4") ) { return "hde"; }
            else if( deviceNumber.equals("5") ) { return "hdf"; }
            else if( deviceNumber.equals("6") ) { return "hdg"; }
            else if( deviceNumber.equals("7") ) { return "hdh"; }
            else if( deviceNumber.equals("8") ) { return "hdi"; }
            else if( deviceNumber.equals("9") ) { return "hdj"; }
            else { return "hdj"; }
        }
    }

    private transient String storageComputeId;
    public String findAvailableStorage(@Nonnull long capacityKB, @Nonnull DataCenter dataCenter) throws CloudException, InternalException {
        APITrace.begin(provider, "Volumes.findStorage");
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                HashMap<String, Integer> map = new HashMap<String, Integer>();
                String obj = method.getString("/Storage?$filter=IsRemoved eq false and Hypervisor/Site/SiteID eq '"+dataCenter.getProviderDataCenterId()+"'", "Volumes.findStorage");
                if (obj != null && obj.length() > 0) {
                    JSONArray list = new JSONArray(obj);
                    for (int i=0; i<list.length(); i++) {
                        boolean found = false;
                        JSONObject json = list.getJSONObject(i);
                        String id = json.getString("StorageID");
                        long freeSpaceKB = json.getLong("FreeSpaceKB");
                        long storageCapacityKB = json.getLong("CapacityKB");
                        int percentFree = Math.round((storageCapacityKB/freeSpaceKB)*100);

                        JSONArray computeIds = json.getJSONArray("ComputeResourceIDs");
                        for (int j = 0; j < computeIds.length(); j++) {
                            String computeID = computeIds.getString(j);
                            if (computeID.equals(storageComputeId)) {
                                found = true;
                                break;
                            }
                        }
                        // as long as the storage has enough free space we can use it
                        if (capacityKB <= freeSpaceKB && found) {
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

    private String getComputeIDFromResourcePool(@Nonnull String resourcePoolID) throws InternalException, CloudException {
        APITrace.begin(provider, "Volumes.findResourcePool");
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                String obj = method.getString("/ResourcePool/"+resourcePoolID+"?$filter=IsRemoved eq false", "Volumes.findResourcePool");

                if (obj != null && obj.length() > 0) {
                    JSONObject json = new JSONObject(obj);

                    String computeId = json.getString("ComputeResourceID");
                    return computeId;
                }
                logger.error("No available resource pool with id "+resourcePoolID);
                throw new CloudException("No available resource pool with id "+resourcePoolID);
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
}
