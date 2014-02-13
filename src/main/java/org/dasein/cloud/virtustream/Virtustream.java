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
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.virtustream.compute.VirtustreamComputeServices;
import org.dasein.cloud.virtustream.network.VirtustreamNetworkServices;
import org.dasein.cloud.virtustream.storage.VirtustreamStorageServices;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;

public class Virtustream extends AbstractCloud {
    static private final Logger logger = getLogger(Virtustream.class);

    static private final String TEST_CONTEXT    = "testContext";
    static private final String WAIT_FOR_TASK   = "waitForTask";

    static private @Nonnull String getLastItem(@Nonnull String name) {
        int idx = name.lastIndexOf('.');

        if (idx < 0) {
            return name;
        } else if (idx == (name.length() - 1)) {
            return "";
        }
        return name.substring(idx + 1);
    }

    static public @Nonnull Logger getLogger(@Nonnull Class<?> cls) {
        String pkg = getLastItem(cls.getPackage().getName());

        if (pkg.equals("virtustream")) {
            pkg = "";
        } else {
            pkg = pkg + ".";
        }
        return Logger.getLogger("dasein.cloud.virtustream.std." + pkg + getLastItem(cls.getName()));
    }

    static public @Nonnull Logger getWireLogger(@Nonnull Class<?> cls) {
        return Logger.getLogger("dasein.cloud.virtustream.wire." + getLastItem(cls.getPackage().getName()) + "." + getLastItem(cls.getName()));
    }

    public Virtustream() {}

    @Nonnull
    @Override
    public String getCloudName() {
        ProviderContext ctx = getContext();
        String name = (ctx == null ? null : ctx.getCloudName());

        return (name == null ? "Virtustream" : name);
    }

    @Nullable
    @Override
    public VirtustreamComputeServices getComputeServices() {
        return new VirtustreamComputeServices(this);
    }

    @Nonnull
    @Override
    public VirtustreamDataCenterServices getDataCenterServices() {
        return new VirtustreamDataCenterServices(this);
    }

    @Nullable
    @Override
    public VirtustreamNetworkServices getNetworkServices() {
        return new VirtustreamNetworkServices(this);
    }

    @Nonnull
    @Override
    public String getProviderName() {
        ProviderContext ctx = getContext();
        String name = (ctx == null ? null : ctx.getProviderName());

        return (name == null ? "Virtustream" : name);
    }

    @Nullable
    @Override
    public synchronized VirtustreamStorageServices getStorageServices() {
        return new VirtustreamStorageServices(this);
    }

    @Override
    public String testContext() {
        APITrace.begin(this, "testContext");
        try {
            ProviderContext ctx = getContext();

            if (ctx == null) {
                logger.warn("No context was provided for testing");
                return null;
            }
            try {
                String username = new String(ctx.getAccessPublic(), "utf-8");
                VirtustreamMethod method = new VirtustreamMethod(this);
                String body = method.getString("User?$filter=UserPrincipalName eq '"+username+"'", TEST_CONTEXT);

                if (body == null) {
                    return null;
                }
                String uuid = VirtustreamMethod.seekValue(body, "TenantID");
                if (logger.isDebugEnabled()) {
                    logger.debug("TenantID=" + uuid);
                }
                if (uuid == null) {
                    logger.warn("No valid UUID was provided in the response during context testing");
                    return null;
                }
                return uuid;
            } catch (Throwable t) {
                logger.error("Error testing Virtustream credentials for " + ctx.getAccountNumber() + ": " + t.getMessage());
                return null;
            }
        } finally {
            APITrace.end();
        }
    }

    public String parseTaskID(@Nonnull JSONObject response) throws InternalException, CloudException {
        try {
            if (response.has("Headers") && !response.isNull("Headers")) {
                JSONObject headers = response.getJSONObject("Headers");
                if (headers.has("MessageId") && !headers.isNull("MessageId")) {
                    String taskId = headers.getString("MessageId");
                    return waitForTaskCompletion(taskId);
                }
            }
            return null;
        }
        catch (JSONException e) {
            logger.error(e);
            throw new InternalException("Unable to parse JSONObject "+e.getMessage());
        }
    }

    public String waitForTaskCompletion(@Nonnull String taskInfoID) throws InternalException, CloudException {
        APITrace.begin(this, WAIT_FOR_TASK);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(this);
                while (true) {
                    try {
                        Thread.sleep(15000L);
                    }
                    catch (InterruptedException ignore) {}
                    String body = method.getString("/TaskInfo/"+taskInfoID, WAIT_FOR_TASK);
                    if (body != null && body.length() > 0) {
                        JSONObject json = new JSONObject(body);
                        int state = json.getInt("State");
                        if (state == 4) {
                            return json.getString("Result");
                        }
                        if (state == 1) {
                            // check if this is a common error
                            JSONObject errors = json.getJSONObject("Errors");
                            Iterator<String> keys = errors.keys();
                            while (keys.hasNext()) {
                                String key = keys.next();
                                if (key.contains("not found")) {
                                    String error = key+": "+errors.getString(key);
                                    logger.error("CloudException: "+error);
                                    throw new CloudException("CloudException: "+error);
                                }
                            }
                            //at this point just return the full error message
                            String error = json.getString("Errors");
                            logger.error(error);
                            throw new CloudException("CloudException: "+error);
                        }
                    }
                    else {
                        return "Task not found";
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

    public String parseStorageTaskID(@Nonnull JSONObject response) throws InternalException, CloudException {
        try {
            if (response.has("QueuedMessageId") && !response.isNull("QueuedMessageId")) {
                String taskId = response.getString("QueuedMessageId");
                return waitForTaskCompletion(taskId);
            }
            return null;
        }
        catch (JSONException e) {
            logger.error(e);
            throw new InternalException("Unable to parse JSONObject "+e.getMessage());
        }
    }
}