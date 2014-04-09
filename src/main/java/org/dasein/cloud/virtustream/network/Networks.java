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

package org.dasein.cloud.virtustream.network;


import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.network.AbstractVLANSupport;
import org.dasein.cloud.network.InternetGateway;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.VLAN;
import org.dasein.cloud.network.VLANCapabilities;
import org.dasein.cloud.network.VLANState;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.virtustream.Virtustream;
import org.dasein.cloud.virtustream.VirtustreamMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

public class Networks extends AbstractVLANSupport {
    static private final Logger logger = Logger.getLogger(Networks.class);
    static private final String CREATE_NETWORK   =   "Network.createNetwork";
    static private final String GET_NETWORK     =   "Network.getNetwork";
    static private final String IS_SUBSCRIBED   =   "Network.isSubscribed";
    static private final String LIST_VLANS      =   "Network.listVlans";
    static private final String LIST_VLAN_STATUS =  "Network.listVlanStatus";

    private Virtustream provider = null;

    public Networks(@Nonnull Virtustream provider) {
        super(provider);
        this.provider = provider;
    }

    private transient volatile NetworkCapabilities capabilities;@Override
    public VLANCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new NetworkCapabilities(provider);
        }
        return capabilities;
    }

    @Nonnull
    @Override
    public String getProviderTermForNetworkInterface(@Nonnull Locale locale) {
        return "NIC";
    }

    @Nonnull
    @Override
    public String getProviderTermForSubnet(@Nonnull Locale locale) {
        return "Network";
    }

    @Nonnull
    @Override
    public String getProviderTermForVlan(@Nonnull Locale locale) {
        return "Network";
    }

    @Override
    public VLAN getVlan(@Nonnull String vlanId) throws CloudException, InternalException {
        APITrace.begin(provider, GET_NETWORK);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                String obj = method.getString("/Network/"+vlanId+"?$filter=IsRemoved eq false", GET_NETWORK);
                if (obj != null && obj.length() > 0) {
                    JSONObject json = new JSONObject(obj);
                    VLAN vlan = toVlan(json);
                    if (vlan != null){
                        return vlan;
                    }
                }
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse JSONObject "+e.getMessage());
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Nullable
    @Override
    public String getAttachedInternetGatewayId(@Nonnull String vlanId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nullable
    @Override
    public InternetGateway getInternetGatewayById(@Nonnull String gatewayId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, IS_SUBSCRIBED);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                method.getString("/Network", IS_SUBSCRIBED);
                return true;
            }
            catch (Throwable ignore){
                return false;
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isVlanDataCenterConstrained() throws CloudException, InternalException {
        return true;
    }

    @Nonnull
    @Override
    public Collection<InternetGateway> listInternetGateways(@Nullable String vlanId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        ArrayList<IPVersion> list = new ArrayList<IPVersion>();
        list.add(IPVersion.IPV4);
        list.add(IPVersion.IPV6);
        return list;
    }

    @Nonnull
    @Override
    public Iterable<ResourceStatus> listVlanStatus() throws CloudException, InternalException {
        APITrace.begin(provider, LIST_VLAN_STATUS);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
                String obj = method.getString("/Network?$filter=IsRemoved eq false", LIST_VLAN_STATUS);
                if (obj != null && obj.length() > 0) {
                    JSONArray json = new JSONArray(obj);
                    for (int i=0; i<json.length(); i++) {
                        JSONObject node = json.getJSONObject(i);

                        if (node.has("NetworkID") && !node.isNull("NetworkID")) {
                            String id = node.getString("NetworkID");
                            ResourceStatus status = new ResourceStatus(id, VLANState.AVAILABLE);
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
    public Iterable<VLAN> listVlans() throws CloudException, InternalException {
        APITrace.begin(provider, LIST_VLANS);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                ArrayList<VLAN> list = new ArrayList<VLAN>();
                String obj = method.getString("/Network?$filter=IsRemoved eq false", LIST_VLAN_STATUS);
                if (obj != null && obj.length() > 0) {
                    JSONArray json = new JSONArray(obj);
                    for (int i=0; i<json.length(); i++) {
                        JSONObject node = json.getJSONObject(i);

                        VLAN vlan = toVlan(node);
                        if (vlan != null) {
                            list.add(vlan);
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
    public void removeInternetGatewayById(@Nonnull String id) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    private VLAN toVlan(@Nonnull JSONObject json) throws InternalException, CloudException {
        try{
            VLAN vlan = new VLAN();
            vlan.setCurrentState(VLANState.AVAILABLE);
            vlan.setSupportedTraffic(new IPVersion[] { IPVersion.IPV4,IPVersion.IPV6 });

            if (json.has("NetworkID") && !json.isNull("NetworkID")) {
                vlan.setProviderVlanId(json.getString("NetworkID"));
            }
            else {
                return null;
            }

            boolean isRemoved = json.getBoolean("IsRemoved");
            if (isRemoved) {
                logger.debug("IsRemoved flag is set so not returning network "+vlan.getProviderVlanId());
                return null;
            }

            if (json.has("Name") && !json.isNull("Name")) {
                vlan.setName(json.getString("Name"));
            }
            else if (json.has("CustomerDefinedName") && !json.isNull("CustomerDefinedName")) {
                vlan.setName(json.getString("CustomerDefinedName"));
            }

            if (json.has("Description") && !json.isNull("Description")) {
                vlan.setDescription(json.getString("Description"));
            }
            if (json.has("TenantID") && !json.isNull("TenantID")) {
                vlan.setProviderOwnerId(json.getString("TenantID"));
            }
            if (json.has("Hypervisor") && !json.isNull("Hypervisor")) {
                JSONObject hv = json.getJSONObject("Hypervisor");
                JSONObject site = hv.getJSONObject("Site");
                vlan.setProviderDataCenterId(site.getString("SiteID"));
                JSONObject r = site.getJSONObject("Region");
                vlan.setProviderRegionId(r.getString("RegionID"));
            }
            String gateway = null;
            if (json.has("Address") && !json.isNull("Address")) {
                gateway = json.getString("Address");
            }
            String netmask = null;
            if (json.has("Mask") && !json.isNull("Mask")) {
                netmask = json.getString("Mask");
            }
            if( gateway != null ) {
                if( netmask == null ) {
                    netmask = "255.255.255.0";
                }
                vlan.setCidr(netmask, gateway);
            }

            if (json.has("ComputeResourceIDs") && !json.isNull("ComputeResourceIDs")) {
                JSONArray list = json.getJSONArray("ComputeResourceIDs");
                vlan.setTag("numComputeIds", Integer.toString(list.length()));
                for ( int i = 0; i<list.length(); i++) {
                    String computeResourceID =  list.getString(i);
                    vlan.setTag("computeResourceID"+i, computeResourceID);
                }
            }

            if (vlan.getName() == null) {
                vlan.setName(vlan.getProviderVlanId());
            }
            if (vlan.getDescription() == null) {
                vlan.setDescription(vlan.getName());
            }
            if (vlan.getProviderRegionId() == null) {
                logger.warn("No region found for "+vlan.getProviderVlanId());
                return null;
            }
            if (vlan.getCidr() == null) {
                vlan.setCidr("0.0.0.0/0");
            }
            return vlan;
        }
        catch (JSONException e) {
            logger.error(e);
            throw new InternalException("Unable to parse JSONObject "+e.getMessage());
        }
    }
}
