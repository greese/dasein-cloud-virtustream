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
import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.compute.AbstractImageSupport;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageCapabilities;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageCreateOptions;
import org.dasein.cloud.compute.ImageFilterOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.virtustream.Virtustream;
import org.dasein.cloud.virtustream.VirtustreamMethod;
import org.dasein.util.CalendarWrapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

public class Templates extends AbstractImageSupport{
    static private final Logger logger = Logger.getLogger(Templates.class);
    static private final String CAPTURE_IMAGE   =   "Image.captureImage";
    static private final String DISCONNECT_NIC  =   "Image.disconnectNic";
    static private final String GET_IMAGE       =   "Image.getImage";
    static private final String IS_SUBSCRIBED   =   "Image.isSubscribed";
    static private final String LIST_IMAGES     =   "Image.listImages";
    static private final String LIST_IMAGE_STATUS = "Image.listImageStatus";
    static private final String REMOVE_TEMPLATE =   "Image.removeTemplate";
    static private final String SEARCH_PUBLIC_IMAGES = "Image.searchPublicImages";

    private Virtustream provider = null;

    public Templates(@Nonnull Virtustream provider) {
        super(provider);
        this.provider = provider;
    }

    private transient volatile TemplateCapabilities capabilities;
    @Override
    public ImageCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new TemplateCapabilities(provider);
        }
        return capabilities;
    }

    @Nullable
    @Override
    public MachineImage getImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(provider, GET_IMAGE);
        try {
            VirtustreamMethod method = new VirtustreamMethod(provider);
            String obj = method.getString("VirtualMachine/"+providerImageId+"?$filter=IsRemoved eq false", GET_IMAGE);
            if (obj != null && obj.length()> 0 ) {
                try {
                    JSONObject json = new JSONObject(obj);
                    MachineImage img = toImage(json);

                    if (img != null) {
                        return img;
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
    protected MachineImage capture(@Nonnull ImageCreateOptions options, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        APITrace.begin(provider, CAPTURE_IMAGE);
        try {
            // create a copy of the vm first and then mark the copy as a template
            String vmid = options.getVirtualMachineId();
            String templateName = options.getName();
            String description = options.getDescription();
            boolean powerOn = false;

            VirtualMachines support = provider.getComputeServices().getVirtualMachineSupport();
            VirtualMachine currentVM = support.getVirtualMachine(vmid);
            VirtualMachine newVM = support.clone(vmid, currentVM.getProviderDataCenterId(), templateName, description, powerOn, currentVM.getProviderFirewallIds());

            VirtustreamMethod method = new VirtustreamMethod(provider);

            //disconnect clone from any networks
            JSONObject nic = new JSONObject();
            try {
                nic.put("VirtualMachineID", newVM.getProviderVirtualMachineId());
                nic.put("VirtualMachineNicID", newVM.getTag("VirtualMachineNicID"));
            }
            catch (JSONException ex) {
                logger.error(ex);
            }

            String response = method.postString("/VirtualMachine/RemoveNic", nic.toString(), DISCONNECT_NIC);
            if (response != null && response.length() > 0) {
                try {
                    JSONObject json = new JSONObject(response);
                    provider.parseTaskID(json);
                }
                catch (JSONException e) {
                    logger.error(e);
                    throw new InternalException("Unable to parse JSON "+e.getMessage());
                }
            }

            //list vm details to check nic was disconnected properly
            support.getVirtualMachine(newVM.getProviderVirtualMachineId());

            String obj = method.postString("/VirtualMachine/MarkAsTemplate", newVM.getProviderVirtualMachineId(), CAPTURE_IMAGE);

            String templateId = null;
            if (obj != null && obj.length() > 0) {
                try {
                    JSONObject json = new JSONObject(obj);
                    templateId = provider.parseTaskID(json);
                }
                catch (JSONException e) {
                    logger.error(e);
                    throw new InternalException("Unable to parse JSON "+e.getMessage());
                }

            }
            if (templateId == null) {
                logger.error("Template created without error but no new id returned");
                throw new CloudException("Template created without error but no new id returned");
            }

            long timeout = System.currentTimeMillis()+(CalendarWrapper.MINUTE *5l);

            MachineImage img = null;
            while (timeout > System.currentTimeMillis()) {
                img = getImage(templateId);
                if (img != null) {
                    break;
                }
                try {
                    Thread.sleep(15000l);
                }
                catch (InterruptedException ignore) {}
            }
            if( img == null ) {
                logger.error("Machine image job completed successfully, but no image " + templateId + " exists.");
                throw new CloudException("Machine image job completed successfully, but no image " + templateId + " exists.");
            }

            if( task != null ) {
                task.completeWithResult(img);
            }
            return img;
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public String getProviderTermForImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        return "template";
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String providerImageId) throws CloudException, InternalException {
        MachineImage img = getImage(providerImageId);
        return img.getTag("isPublic").equals("true");
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, IS_SUBSCRIBED);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                ArrayList<MachineImage> list = new ArrayList<MachineImage>();
                method.getString("VirtualMachine?$filter=IsTemplate eq true and IsRemoved eq false", LIST_IMAGES);
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
    public Iterable<ResourceStatus> listImageStatus(@Nonnull ImageClass cls) throws CloudException, InternalException {
        APITrace.begin(provider, LIST_IMAGE_STATUS);
        try {
            if( !cls.equals(ImageClass.MACHINE) ) {
                return Collections.emptyList();
            }
            VirtustreamMethod method = new VirtustreamMethod(provider);
            ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
            String obj = method.getString("VirtualMachine?$filter=IsTemplate eq true and IsRemoved eq false and IsGlobalTemplate eq false", LIST_IMAGES);
            if (obj != null && obj.length() > 0) {
                JSONArray json = null;
                JSONObject node = null;

                try {
                    json = new JSONArray(obj);
                }
                catch (JSONException e) {
                    logger.error(e);
                    throw new InternalException("Unable to parse JSON "+e.getMessage());
                }

                for (int i=0; i<json.length(); i++) {
                    try {
                        node = json.getJSONObject(i);
                        String imageId = null;

                        imageId = node.getString("VirtualMachineID");
                        if (imageId == null) {
                            logger.error("Found a template without an id");
                            continue;
                        }

                        //check this is indeed a template
                        boolean isTemplate = node.getBoolean("IsTemplate");
                        if (!isTemplate) {
                            logger.error("Resource with id "+imageId+" is not a template");
                            continue;
                        }

                        boolean isRemoved = node.getBoolean("IsRemoved");
                        if (isRemoved) {
                            logger.debug("IsRemoved flag set so not returning template "+imageId);
                            continue;
                        }
                        if (!node.has("TenantID") || node.isNull("TenantID")) {
                            logger.warn("No tenant id found for "+imageId);
                            continue;
                        }

                        ResourceStatus status = new ResourceStatus(imageId, MachineImageState.ACTIVE);
                        list.add(status);
                    }
                    catch (JSONException e) {
                        logger.error(e);
                        throw new InternalException("Unable to parse JSONObject "+node);
                    }
                }
            }
            return list;
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public Iterable<MachineImage> listImages(@Nullable ImageFilterOptions options) throws CloudException, InternalException {
        APITrace.begin(provider, GET_IMAGE);
        try {
            VirtustreamMethod method = new VirtustreamMethod(provider);
            ArrayList<MachineImage> list = new ArrayList<MachineImage>();
            String obj = method.getString("VirtualMachine?$filter=IsTemplate eq true and IsRemoved eq false and TenantID eq '"+getContext().getAccountNumber()+"'", LIST_IMAGES);
            if (obj != null && obj.length() > 0) {
                JSONArray json = null;
                try {
                    json = new JSONArray(obj);
                    for (int i=0; i<json.length(); i++) {
                        MachineImage img = toImage(json.getJSONObject(i));

                        if (img != null && (options == null || options.matches(img))) {
                            list.add(img);
                        }
                    }
                }
                catch (JSONException e) {
                    logger.error(e);
                    throw new InternalException("Unable to parse JSON "+e.getMessage());
                }
            }
            return list;

        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public Iterable<MachineImage> searchPublicImages(@Nonnull ImageFilterOptions options) throws CloudException, InternalException {
        APITrace.begin(provider, SEARCH_PUBLIC_IMAGES);
        try {
            VirtustreamMethod method = new VirtustreamMethod(provider);
            ArrayList<MachineImage> list = new ArrayList<MachineImage>();

            JSONArray json;
            String obj = method.getString("VirtualMachine?$filter=IsGlobalTemplate eq true and IsRemoved eq false", SEARCH_PUBLIC_IMAGES);
            if (obj != null && obj.length() > 0) {
                try {
                    json = new JSONArray(obj);
                    for (int i = 0; i<json.length(); i++) {
                        MachineImage img = toImage(json.getJSONObject(i));
                        if (img != null && (options == null || options.matches(img))) {
                            list.add(img);
                        }
                    }
                }
                catch (JSONException e) {
                    logger.error(e);
                    throw new InternalException("Unable to parse JSON "+e.getMessage());
                }
            }
            return list;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean supportsCustomImages() throws CloudException, InternalException {
        return true;
    }

    @Override
    public void remove(@Nonnull String providerImageId, boolean checkState) throws CloudException, InternalException {
        APITrace.begin(provider, REMOVE_TEMPLATE);
        try {
            VirtustreamMethod method = new VirtustreamMethod(provider);
            String obj = method.postString("VirtualMachine/RemoveTemplate", providerImageId, REMOVE_TEMPLATE);
            if (obj != null && obj.length() > 0) {
                JSONObject json;
                try {
                    json = new JSONObject(obj);
                    if (provider.parseTaskID(json) == null) {
                        logger.warn("No confirmation of RemoveTemplate task completion but no error either");
                    }
                }
                catch (JSONException e) {
                    logger.error(e);
                    throw new InternalException("Unable to parse JSON "+e.getMessage());
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    private MachineImage toImage(@Nonnull JSONObject node) throws InternalException, CloudException {
        try {
            String ownerId = null;
            String regionId = null;
            String imageId;
            MachineImageState state = MachineImageState.ACTIVE;
            String name = null;
            String description = null;
            Architecture architecture;
            Platform platform;
            String datacenterId = null;
            HashMap<String,String> properties = new HashMap<String,String>();

            imageId = node.getString("VirtualMachineID");
            if (imageId == null) {
                logger.error("Found a template without an id");
                return null;
            }

            //check this is indeed a template
            boolean isTemplate = node.getBoolean("IsTemplate");
            if (!isTemplate) {
                logger.error("Resource with id "+imageId+" is not a template");
                return null;
            }

            boolean isRemoved = node.getBoolean("IsRemoved");
            if (isRemoved) {
                logger.debug("IsRemoved flag set so not returning template "+imageId);
                return null;
            }

            if (node.has("CustomerDefinedName") && !node.isNull("CustomerDefinedName")) {
                name = node.getString("CustomerDefinedName");
            }
            if (node.has("Description") && !node.isNull("Description")) {
                description = node.getString("Description");
            }

            platform = Platform.guess(node.getString("OS"));
            architecture = guess(node.getString("OSFullName"));

            if (node.has("TenantID") && !node.isNull("TenantID")) {
                ownerId = node.getString("TenantID");
            }
            else {
                //no owner id so this template may not be stable
                return null;
            }
            if (node.has("RegionID") && !node.isNull("RegionID")) {
                regionId = node.getString("RegionID");
            }
            properties.put("isPublic", node.getBoolean("IsGlobalTemplate") == true ? "true" : "false");

            if (node.has("Hypervisor") && !node.isNull("Hypervisor")) {
                JSONObject hv = node.getJSONObject("Hypervisor");
                JSONObject site = hv.getJSONObject("Site");
                datacenterId = site.getString("SiteID");
                if (regionId == null || regionId.equals("0")) {
                    //get region from hypervisor site
                    JSONObject r = site.getJSONObject("Region");
                    regionId = r.getString("RegionID");
                }
            }

            if (node.has("Disks") && !node.isNull("Disks")) {
                JSONArray disks = node.getJSONArray("Disks");
                JSONObject disk = disks.getJSONObject(0);
                int deviceKey = disk.getInt("DeviceKey");
                properties.put("DeviceKey", Integer.toString(deviceKey));
            }

            if (regionId == null) {
                logger.error("Unable to find region id for template "+imageId);
                return null;
            }
            else {
                if (!regionId.equals(getContext().getRegionId())) {
                    return null;
                }
            }
            if (name == null) {
                name = imageId;
            }
            if (description == null) {
                description = name;
            }

            MachineImage img = MachineImage.getMachineImageInstance(ownerId, regionId, imageId, state, name, description, architecture, platform);
            img.setTags(properties);
            if (datacenterId != null) {
                img.constrainedTo(datacenterId);
            }
            return img;
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
}
