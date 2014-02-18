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

package org.dasein.cloud.virtustream;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Region;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.time.Minute;
import org.dasein.util.uom.time.TimePeriod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

public class VirtustreamDataCenterServices implements DataCenterServices {
    static private final Logger logger = Logger.getLogger(VirtustreamDataCenterServices.class);

    static private final String GET_DATA_CENTER     =   "DC.getDataCenter";
    static private final String GET_REGION          =   "DC.getRegion";
    static private final String LIST_DATACENTERS    =   "DC.listDataCenters";
    static private final String LIST_REGIONS        =   "DC.listRegions";

    private Virtustream provider;

    public VirtustreamDataCenterServices(Virtustream provider) { this.provider = provider; }

    @Override
    public DataCenter getDataCenter(String providerDataCenterId) throws InternalException, CloudException {
        APITrace.begin(provider, GET_DATA_CENTER);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);

                String obj = method.getString("Site/"+providerDataCenterId,GET_DATA_CENTER);
                if (obj != null && obj.length() > 0) {
                    JSONObject json = new JSONObject(obj);
                    DataCenter dc = toDataCenter(json);
                    if (dc != null) {
                        return dc;
                    }
                }
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse json response "+e.getMessage());
            }

            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public String getProviderTermForDataCenter(Locale locale) {
        return "Site";
    }

    @Override
    public String getProviderTermForRegion(Locale locale) {
        return "Region";
    }

    @Override
    public Region getRegion(String providerRegionId) throws InternalException, CloudException {
        APITrace.begin(provider, GET_REGION);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                String obj = method.getString("Region/"+providerRegionId,GET_REGION);

                if (obj != null && obj.length() > 0) {
                    JSONObject json = new JSONObject(obj);
                    Region r = toRegion(json);

                    if (r != null) {
                        return r;
                    }
                }
                return null;
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
    public Collection<DataCenter> listDataCenters(String providerRegionId) throws InternalException, CloudException {
        APITrace.begin(provider, LIST_DATACENTERS);
        try {
            Cache<DataCenter> cache = Cache.getInstance(provider, "dataCenters", DataCenter.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Minute>(15, TimePeriod.MINUTE));
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was set for this request");
                throw new CloudException("No context was set for this request");
            }
            Collection<DataCenter> dcs = (Collection<DataCenter>)cache.get(ctx);

            if( dcs == null ) {
                try {
                    VirtustreamMethod method = new VirtustreamMethod(provider);
                    dcs = new ArrayList<DataCenter>();
                    String obj = method.getString("Site?$filter=Region/RegionID eq '"+providerRegionId+"'", LIST_DATACENTERS);

                    if (obj != null && obj.length()> 0) {
                        JSONArray json =  new JSONArray(obj);
                        for (int i=0; i<json.length(); i++) {
                            DataCenter dc = toDataCenter(json.getJSONObject(i));

                            if (dc != null) {
                                dcs.add(dc);
                            }
                        }
                    }
                    cache.put(ctx, dcs);
                }
                catch (JSONException e) {
                    logger.error(e);
                    throw new InternalException("Unable to parse JSONObject "+e.getMessage());
                }
            }
            return dcs;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Collection<Region> listRegions() throws InternalException, CloudException {
        APITrace.begin(provider, LIST_REGIONS);
        try {
            Cache<Region> cache = Cache.getInstance(provider, "regions", Region.class, CacheLevel.CLOUD_ACCOUNT, new TimePeriod<Minute>(15, TimePeriod.MINUTE));
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context was set for this request");
                throw new CloudException("No context was set for this request");
            }
            Collection<Region> regions = (Collection<Region>)cache.get(ctx);

            if( regions == null ) {
                try {
                    VirtustreamMethod method = new VirtustreamMethod(provider);
                    regions = new ArrayList<Region>();
                    String obj = method.getString("Region", LIST_REGIONS);

                    if (obj != null && obj.length() > 0) {
                        JSONArray nodes = new JSONArray(obj);
                        for (int i=0; i<nodes.length(); i++) {
                            Region r = toRegion(nodes.getJSONObject(i));
                            if (r != null) {
                                regions.add(r);
                            }
                        }
                    }
                    cache.put(ctx, regions);
                }
                catch (JSONException e) {
                    logger.error(e);
                    throw new InternalException("Unable to parse JSONObject "+e.getMessage());
                }
            }
            return regions;
        }
        finally {
            APITrace.end();
        }
    }

    private DataCenter toDataCenter(@Nonnull JSONObject json) throws InternalException, CloudException {
        try {
            DataCenter dc = new DataCenter();

            if (json.has("Active") && !json.isNull("Active")) {
                dc.setActive(json.getBoolean("Active"));
                dc.setAvailable(dc.isActive());
            }
            if (json.has("Name") && !json.isNull("Name")) {
                dc.setName(json.getString("Name"));
            }
            if (json.has("SiteID") && !json.isNull("SiteID")) {
                dc.setProviderDataCenterId(json.getString("SiteID"));
            }
            if (json.has("Region") && !json.isNull("Region")) {
                JSONObject region = json.getJSONObject("Region");
                if (region.has("RegionID") && !json.has("RegionID")) {
                    dc.setRegionId(region.getString("RegionID"));
                }
            }
            if (dc.getRegionId() != null && dc.getProviderDataCenterId() != null) {
                return dc;
            }
            logger.error("Found a datacenter with no id or region id");
            return null;
        }
        catch (JSONException e) {
            logger.error(e);
            throw new InternalException("Unable to parse JSONObject "+e.getMessage());
        }
    }

    private Region toRegion(@Nonnull JSONObject json) throws InternalException, CloudException {
        try {
            Region r = new Region();
            r.setJurisdiction("US");

            if (json.has("Active") && !json.isNull("Active")) {
                r.setActive(json.getBoolean("Active"));
                r.setAvailable(r.isActive());
            }
            if (json.has("Name") && !json.isNull("Name")) {
                r.setName(json.getString("Name"));
            }
            if (json.has("RegionID") && !json.isNull("RegionID")) {
                r.setProviderRegionId(json.getString("RegionID"));
            }
            if (r.getProviderRegionId() != null) {
                return r;
            }
            logger.error("Found a region with no id");
            return null;
        }
        catch (JSONException e) {
            logger.error(e);
            throw new InternalException("Unable to parse JSONObject "+e.getMessage());
        }
    }
}
