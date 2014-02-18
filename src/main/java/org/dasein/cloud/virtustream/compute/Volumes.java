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
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.compute.AbstractVolumeSupport;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.Volume;
import org.dasein.cloud.compute.VolumeFilterOptions;
import org.dasein.cloud.compute.VolumeFormat;
import org.dasein.cloud.compute.VolumeState;
import org.dasein.cloud.compute.VolumeType;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.virtustream.Virtustream;
import org.dasein.cloud.virtustream.VirtustreamMethod;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Kilobyte;
import org.dasein.util.uom.storage.Storage;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Locale;

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
        return null;
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

                        JSONArray disks = json.getJSONArray("Disks");
                        for (int j=0; j<disks.length(); j++) {
                            JSONObject diskJson = disks.getJSONObject(j);

                            // create Volume object
                            Volume volume = toVolume(vmID, regionID, dataCenterID, diskJson);
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

                String vmID = getVolume(volumeId).getProviderVirtualMachineId();
                json.put("VirtualMachineID", vmID);

                String body = json.toString();
                String obj = method.postString("/VirtualMachine/RemoveDisk", body, REMOVE_VOLUMES);
                if (obj != null && obj.length() > 0) {
                    JSONObject response = new JSONObject(obj);
                    if (provider.parseTaskID(response) == null) {
                        logger.warn("No confirmation of RemoveVolume task completion but no error either");
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
        }
        catch (JSONException e) {
            logger.error(e);
            throw new InternalException("Unable to parse JSONObject "+e.getMessage());
        }
    }

    private Volume toVolume(@Nonnull String vmID, @Nonnull String regionID, @Nonnull String dataCenterID, @Nonnull JSONObject json) throws InternalException, CloudException {
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

            return volume;
        }
        catch (JSONException e) {
            logger.error(e);
            throw new InternalException("Unable to parse JSONObject "+e.getMessage());
        }
    }
}