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

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.util.APITrace;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Properties;

public class VirtustreamStorageMethod {
    static private final Logger logger = Virtustream.getLogger(VirtustreamStorageMethod.class);
    static private final Logger wire = Virtustream.getWireLogger(VirtustreamStorageMethod.class);

    /**
     * 200	OK	Command succeeded, data returned (possibly 0 length)
     */
    static public final int OK = 200;

    /**
     * 202	ACCEPTED	Command accepted for asynchronous processing, data returned (possibly 0 length)
     */
    static public final int CREATED = 201;

    /**
     * 202	ACCEPTED	Command accepted for asynchronous processing, data returned (possibly 0 length)
     */
    static public final int ACCEPTED = 202;

    /**
     * 204	No Content	Command succeeded, no data returned (by definition)
     */
    static public final int NO_CONTENT = 204;

    /**
     * 400	Bad Request
     */
    static public final int BAD_REQUEST = 400;

    /**
     * 404	Not Found	Command, drive, server or other object not found
     */
    static public final int NOT_FOUND = 404;

    private Virtustream provider;

    public VirtustreamStorageMethod(@Nonnull Virtustream provider) {
        this.provider = provider;
    }

    private @Nonnull
    HttpClient getClient(URI uri) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if (ctx == null) {
            throw new InternalException();
        }
        boolean ssl = uri.getScheme().startsWith("https");
        HttpParams params = new BasicHttpParams();

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        //noinspection deprecation
        HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);
        HttpProtocolParams.setUserAgent(params, "");

        Properties p = ctx.getCustomProperties();

        if (p != null) {
            String proxyHost = p.getProperty("proxyHost");
            String proxyPort = p.getProperty("proxyPort");

            if (proxyHost != null) {
                int port = 0;

                if (proxyPort != null && proxyPort.length() > 0) {
                    port = Integer.parseInt(proxyPort);
                }
                params.setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(proxyHost, port, ssl ? "https" : "http"));
            }
        }
        return new DefaultHttpClient(params);
    }

    private @Nonnull String getEndpoint(@Nonnull String resource) throws InternalException {
        ProviderContext ctx = provider.getContext();
        String target = "";

        if (ctx == null) {
            throw new InternalException();
        }
        String endpoint = ctx.getEndpoint();

        if (endpoint == null || endpoint.trim().equals("")) {
            throw new InternalException("Context endpoint not set");
        }
        if (resource.startsWith("/")) {
            while (endpoint.endsWith("/") && !endpoint.equals("/")) {
                endpoint = endpoint.substring(0, endpoint.length() - 1);
            }
            target = endpoint + resource;
        } else if (endpoint.endsWith("/")) {
            target = endpoint + resource;
        }
        else {
            target = endpoint + "/" + resource;
        }

        return target;
    }

    public @Nullable
    String postString(@Nonnull String resource, @Nonnull String body, @Nullable String sessionId, @Nonnull String command) throws InternalException, CloudException {
        if (logger.isTraceEnabled()) {
            logger.trace("ENTER - " + Virtustream.class.getName() + ".postString(" + resource + "," + body + ")");
        }

        try {
            String target = getEndpoint(resource);
            if (wire.isDebugEnabled()) {
                wire.debug("");
                wire.debug(">>> [POST (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                URI uri;

                try {
                    target = target.replace(" ", "%20");
                    uri = new URI(target);
                } catch (URISyntaxException e) {
                    throw new InternalException(e);
                }
                HttpClient client = getClient(uri);

                try {
                    ProviderContext ctx = provider.getContext();

                    if (ctx == null) {
                        throw new InternalException("No context was set");
                    }
                    HttpPost post = new HttpPost(target);

                    post.addHeader("Content-Type", "application/json; charset=utf-8");
                    post.addHeader("Accept", "application/json");

                    if (sessionId != null) {
                        post.addHeader("Cookie", "xs-session="+sessionId);
                    }
                    try {
                        post.setEntity(new StringEntity(body, "utf-8"));
                    } catch (UnsupportedEncodingException e) {
                        logger.error("Unsupported encoding UTF-8: " + e.getMessage());
                        throw new InternalException(e);
                    }

                    if (wire.isDebugEnabled()) {
                        wire.debug(post.getRequestLine().toString());
                        for (Header header : post.getAllHeaders()) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                        wire.debug(body);
                        wire.debug("");
                    }
                    HttpResponse response;
                    StatusLine status;

                    try {
                        APITrace.trace(provider, command);
                        response = client.execute(post);
                        status = response.getStatusLine();
                    } catch (IOException e) {
                        logger.error("Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("HTTP Status " + status);
                    }
                    Header[] headers = response.getAllHeaders();

                    if (wire.isDebugEnabled()) {
                        wire.debug(status.toString());
                        for (Header h : headers) {
                            if (h.getValue() != null) {
                                wire.debug(h.getName() + ": " + h.getValue().trim());
                            } else {
                                wire.debug(h.getName() + ":");
                            }
                        }
                        wire.debug("");
                    }
                    if (status.getStatusCode() == NOT_FOUND) {
                        return null;
                    }
                    if (status.getStatusCode() != OK && status.getStatusCode() != NO_CONTENT && status.getStatusCode() != CREATED && status.getStatusCode() != ACCEPTED) {
                        logger.error("Expected OK for POST request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();

                        if (entity == null) {
                            throw new VirtustreamException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        } catch (IOException e) {
                            throw new VirtustreamException(e);
                        }
                        if (wire.isDebugEnabled()) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        throw new VirtustreamException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), body);
                    } else {
                        HttpEntity entity = response.getEntity();

                        if (entity == null) {
                            return "";
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        } catch (IOException e) {
                            throw new VirtustreamException(e);
                        }
                        if (wire.isDebugEnabled()) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        return body;
                    }
                } finally {
                    try {
                        client.getConnectionManager().shutdown();
                    } catch (Throwable ignore) {
                    }
                }
            } finally {
                if (wire.isDebugEnabled()) {
                    wire.debug("<<< [POST (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        } finally {
            if (logger.isTraceEnabled()) {
                logger.trace("EXIT - " + Virtustream.class.getName() + ".postString()");
            }
        }
    }

    public @Nullable
    InputStream getFileDownload(@Nonnull String resource, @Nonnull String sessionID, @Nonnull String command) throws InternalException, CloudException {
        if (logger.isTraceEnabled()) {
            logger.trace("ENTER - " + Virtustream.class.getName() + ".getFileDownload(" + resource + ")");
        }

        try {
            String body = "";
            String target = getEndpoint(resource);
            if (wire.isDebugEnabled()) {
                wire.debug("");
                wire.debug(">>> [GET (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                URI uri;

                try {
                    target = target.replace(" ", "%20");
                    uri = new URI(target);
                } catch (URISyntaxException e) {
                    throw new InternalException(e);
                }
                HttpClient client = getClient(uri);

                try {
                    ProviderContext ctx = provider.getContext();

                    if (ctx == null) {
                        throw new InternalException("No context was set");
                    }
                    HttpGet get = new HttpGet(target);

                    get.addHeader("Content-Type", "application/json; charset=utf-8");
                    get.addHeader("Accept", "application/json");
                    get.addHeader("Cookie", "xs-session=" + sessionID);

                    if (wire.isDebugEnabled()) {
                        wire.debug(get.getRequestLine().toString());
                        for (Header header : get.getAllHeaders()) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                        wire.debug("");
                    }
                    HttpResponse response;
                    StatusLine status;

                    try {
                        APITrace.trace(provider, command);
                        response = client.execute(get);
                        status = response.getStatusLine();
                    } catch (IOException e) {
                        logger.error("Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("HTTP Status " + status);
                    }
                    Header[] headers = response.getAllHeaders();

                    if (wire.isDebugEnabled()) {
                        wire.debug(status.toString());
                        for (Header h : headers) {
                            if (h.getValue() != null) {
                                wire.debug(h.getName() + ": " + h.getValue().trim());
                            } else {
                                wire.debug(h.getName() + ":");
                            }
                        }
                        wire.debug("");
                    }
                    if (status.getStatusCode() == NOT_FOUND) {
                        return null;
                    }
                    if (status.getStatusCode() != OK && status.getStatusCode() != NO_CONTENT && status.getStatusCode() != CREATED && status.getStatusCode() != ACCEPTED) {
                        logger.error("Expected OK for POST request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();

                        if (entity == null) {
                            throw new VirtustreamException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        } catch (IOException e) {
                            throw new VirtustreamException(e);
                        }
                        if (wire.isDebugEnabled()) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        throw new VirtustreamException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), body);
                    } else {
                        HttpEntity entity = response.getEntity();

                        if (entity == null) {
                            return null;
                        }
                        InputStream input;
                        try {
                            input = entity.getContent();
                        } catch (IOException e) {
                            throw new VirtustreamException(e);
                        }

                        return input;
                    }
                } finally {
                    try {
                        client.getConnectionManager().shutdown();
                    } catch (Throwable ignore) {
                    }
                }
            } finally {
                if (wire.isDebugEnabled()) {
                    wire.debug("<<< [GET (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        } finally {
            if (logger.isTraceEnabled()) {
                logger.trace("EXIT - " + Virtustream.class.getName() + ".getFileDownload()");
            }
        }
    }

    public @Nullable
    String deleteString(@Nonnull String resource, @Nullable String sessionId, @Nonnull String command) throws InternalException, CloudException {
        if (logger.isTraceEnabled()) {
            logger.trace("ENTER - " + Virtustream.class.getName() + ".deleteString(" + resource+ ")");
        }

        try {
            String target = getEndpoint(resource);
            if (wire.isDebugEnabled()) {
                wire.debug("");
                wire.debug(">>> [DELETE (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                URI uri;

                try {
                    target = target.replace(" ", "%20");
                    uri = new URI(target);
                } catch (URISyntaxException e) {
                    throw new InternalException(e);
                }
                HttpClient client = getClient(uri);

                try {
                    ProviderContext ctx = provider.getContext();

                    if (ctx == null) {
                        throw new InternalException("No context was set");
                    }
                    HttpDelete delete = new HttpDelete(target);

                    delete.addHeader("Content-Type", "application/json; charset=utf-8");
                    delete.addHeader("Accept", "application/json");

                    if (sessionId != null) {
                        delete.addHeader("Cookie", "xs-session=" + sessionId);
                    }
                    if (wire.isDebugEnabled()) {
                        wire.debug(delete.getRequestLine().toString());
                        for (Header header : delete.getAllHeaders()) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                        wire.debug("");
                    }
                    HttpResponse response;
                    StatusLine status;

                    try {
                        APITrace.trace(provider, command);
                        response = client.execute(delete);
                        status = response.getStatusLine();
                    } catch (IOException e) {
                        logger.error("Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("HTTP Status " + status);
                    }
                    Header[] headers = response.getAllHeaders();

                    if (wire.isDebugEnabled()) {
                        wire.debug(status.toString());
                        for (Header h : headers) {
                            if (h.getValue() != null) {
                                wire.debug(h.getName() + ": " + h.getValue().trim());
                            } else {
                                wire.debug(h.getName() + ":");
                            }
                        }
                        wire.debug("");
                    }
                    if (status.getStatusCode() == NOT_FOUND) {
                        return null;
                    }
                    if (status.getStatusCode() != OK && status.getStatusCode() != NO_CONTENT && status.getStatusCode() != CREATED && status.getStatusCode() != ACCEPTED) {
                        logger.error("Expected OK for POST request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();

                        String body;
                        if (entity == null) {
                            throw new VirtustreamException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        } catch (IOException e) {
                            throw new VirtustreamException(e);
                        }
                        if (wire.isDebugEnabled()) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        throw new VirtustreamException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), body);
                    } else {
                        HttpEntity entity = response.getEntity();

                        String body;
                        if (entity == null) {
                            return "";
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        } catch (IOException e) {
                            throw new VirtustreamException(e);
                        }
                        if (wire.isDebugEnabled()) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        return body;
                    }
                } finally {
                    try {
                        client.getConnectionManager().shutdown();
                    } catch (Throwable ignore) {
                    }
                }
            } finally {
                if (wire.isDebugEnabled()) {
                    wire.debug("<<< [POST (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        } finally {
            if (logger.isTraceEnabled()) {
                logger.trace("EXIT - " + Virtustream.class.getName() + ".postString()");
            }
        }
    }

    private String parseError(@Nonnull String body) {
        try {
            JSONObject obj = new JSONObject(body);
            if (obj.has("ResponseStatus") && !obj.isNull("ResponseStatus")) {
                JSONObject r = obj.getJSONObject("ResponseStatus");
                if (r.has("Message") && !r.isNull("Message")) {
                    return r.getString("Message");
                }
            }
        }
        catch (JSONException e) {
            logger.error(e);
            return null;
        }
        return null;
    }
}
