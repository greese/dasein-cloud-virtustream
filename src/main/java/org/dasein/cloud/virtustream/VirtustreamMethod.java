package org.dasein.cloud.virtustream;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
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
import org.json.JSONArray;
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

public class VirtustreamMethod {
    static private final Logger logger = Virtustream.getLogger(VirtustreamMethod.class);
    static private final Logger wire = Virtustream.getWireLogger(VirtustreamMethod.class);

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

    public VirtustreamMethod(@Nonnull Virtustream provider) {
        this.provider = provider;
    }

    static public @Nullable String seekValue(@Nonnull String body, @Nonnull String key) {
        body = body.trim();
        if (body.length() > 0) {
            try {
                JSONArray obj = new JSONArray(body);
                if (obj != null) {
                    JSONObject json = obj.getJSONObject(0);
                    return json.getString(key);
                }
            }
            catch (JSONException e) {
                logger.error("Exception getting value: "+e.getMessage());
            }
        }
        return null;
    }

    private @Nonnull HttpClient getClient(URI uri) throws InternalException, CloudException {
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

    public @Nullable String getString(@Nonnull String resource, @Nonnull String command) throws InternalException, CloudException {
        if (logger.isTraceEnabled()) {
            logger.trace("ENTER - " + Virtustream.class.getName() + ".getString(" + resource + ")");
        }

        try {
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
                        throw new InternalException();
                    }
                    HttpGet get = new HttpGet(target);
                    String auth;

                    try {
                        String userName = new String(ctx.getAccessPublic(), "utf-8");
                        String password = new String(ctx.getAccessPrivate(), "utf-8");

                        auth = new String(Base64.encodeBase64((userName + ":" + password).getBytes()));
                    } catch (UnsupportedEncodingException e) {
                        throw new InternalException(e);
                    }
                    get.addHeader("Content-Type", "application/json; charset=utf-8");
                    get.addHeader("Accept", "application/json");
                    get.addHeader("Authorization", "Basic " + auth);

                    if (wire.isDebugEnabled()) {
                        wire.debug(get.getRequestLine().toString());
                        for (Header header : get.getAllHeaders()) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
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
                    if (status.getStatusCode() != OK && status.getStatusCode() != NO_CONTENT) {
                        logger.error("Expected OK for GET request, got " + status.getStatusCode());
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
                        if (status.getStatusCode() == BAD_REQUEST && body.contains("could not be found")) {
                            return null;
                        }

                        String errorMessage = parseError(body);

                        if (errorMessage != null && errorMessage.length() > 0) {
                            throw new VirtustreamException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), errorMessage);
                        }
                        throw new VirtustreamException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), errorMessage);
                    } else {
                        HttpEntity entity = response.getEntity();

                        if (entity == null) {
                            return "";
                        }
                        String body;

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
                    wire.debug("<<< [GET (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        } finally {
            if (logger.isTraceEnabled()) {
                logger.trace("EXIT - " + Virtustream.class.getName() + ".getString()");
            }
        }
    }

    public @Nullable
    String postString(@Nonnull String resource, @Nonnull String body, @Nonnull String command) throws InternalException, CloudException {
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
                    String auth;

                    try {
                        String userName = new String(ctx.getAccessPublic(), "utf-8");
                        String password = new String(ctx.getAccessPrivate(), "utf-8");

                        auth = new String(Base64.encodeBase64((userName + ":" + password).getBytes()));
                    } catch (UnsupportedEncodingException e) {
                        throw new InternalException(e);
                    }
                    post.addHeader("Content-Type", "application/json; charset=utf-8");
                    post.addHeader("Accept", "application/json");
                    post.addHeader("Authorization", "Basic " + auth);
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