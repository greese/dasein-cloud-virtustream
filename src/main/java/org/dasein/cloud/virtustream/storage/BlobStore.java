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

package org.dasein.cloud.virtustream.storage;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.NameRules;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.storage.AbstractBlobStoreSupport;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.FileTransfer;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.virtustream.Virtustream;
import org.dasein.cloud.virtustream.VirtustreamMethod;
import org.dasein.cloud.virtustream.VirtustreamStorageMethod;
import org.dasein.util.uom.storage.Storage;
import org.dasein.util.uom.storage.Byte;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;

public class BlobStore extends AbstractBlobStoreSupport{
    static private final Logger logger = Logger.getLogger(BlobStore.class);

    static private final String CANCEL_DOWNLOAD                 =   "Blob.cancelDownload";
    static private final String CANCEL_UPLOAD                   =   "Blob.cancelUpload";
    static private final String DOWNLOAD_FILE                   =   "Blob.downloadFile";
    static private final String CHECK_EXISTS                    =   "Blob.exists";
    static private final String FIND_STORAGE_ID                 =   "BlobStore.findStorageId";
    static private final String GET_BUCKETS                     =   "Blob.getBucket";
    static private final String GET_FILE_TRANSFER_SESSION       =   "Blob.getFileTransferSession";
    static private final String GET_OBJECT                      =   "Blob.getObject";
    static private final String GET_OBJECT_SIZE                 =   "Blob.getObjectSize";
    static private final String IS_SUBSCRIBED                   =   "BlobStore.isSubscribed";
    static private final String LIST_STORAGE                    =   "BlobStore.listStorage";
    static private final String REMOVE_OBJECT                   =   "Blob.removeObject";
    static private final String RENAME_BUCKET                   =   "Blob.renameBucket";
    static private final String UPLOAD_FILE                     =   "Blob.uploadFile";

    private Virtustream provider;

    public BlobStore(Virtustream provider) { this.provider = provider; }


    @Override
    public boolean allowsNestedBuckets() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean allowsRootObjects() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsPublicSharing() throws CloudException, InternalException {
        return false;
    }

    @Nonnull
    @Override
    public Blob createBucket(@Nonnull String bucket, boolean findFreeName) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Bucket creation is not currently supported for " + provider.getCloudName());
    }

    @Override
    public boolean exists(@Nonnull String bucket) throws InternalException, CloudException {
        APITrace.begin(provider, CHECK_EXISTS);
        try {
            try {
                if (bucket.endsWith("/")) {
                    return false;
                }
                VirtustreamMethod method = new VirtustreamMethod(provider);
                int position = bucket.indexOf("/");
                String tmp;
                String path;
                String pattern = "*";
                if (position>0) {
                    tmp = bucket.substring(0, bucket.indexOf("/"));
                    path = bucket.substring(bucket.indexOf("/"), bucket.lastIndexOf("/"));
                    if (path.equals("")) {
                        path = "/";
                    }
                    pattern = bucket.substring(bucket.lastIndexOf("/"));
                }
                else {
                    tmp = bucket;
                    path = "/";
                }

                try {
                    findStorageObjectForName(tmp);
                }
                catch (Throwable ignore) {
                    return false;
                }
                if (pattern.equals("*")) {
                    //we are looking at top level storage bin so if we get this far it exists
                    return true;
                }

                JSONObject body = new JSONObject();
                body.put("StorageID", storageID);
                body.put("Path", path);
                body.put("Pattern", pattern);
                String obj = method.postString("/Storage/StorageSearchFile", body.toString(),LIST_STORAGE);
                if (obj != null && obj.length()> 0) {
                    JSONObject json = new JSONObject(obj);
                    String response = provider.parseTaskID(json);
                    if (response != null && response.length() > 2) {
                        return true;
                    }
                }
                return false;
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
    public Blob getBucket(@Nonnull String bucketName) throws InternalException, CloudException {
        APITrace.begin(provider, GET_BUCKETS);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                int position = bucketName.indexOf("/");
                String tmp;
                String path;
                String pattern = "*";
                boolean root = false;
                if (position>0) {
                    tmp = bucketName.substring(0, bucketName.indexOf("/"));
                    path = bucketName.substring(bucketName.indexOf("/"), bucketName.lastIndexOf("/"));
                    if (path.equals("")) {
                        path = "/";
                    }
                    pattern = bucketName.substring(bucketName.lastIndexOf("/"));
                }
                else {
                    tmp = bucketName;
                    path = "/";
                    root = true;
                }

                findStorageObjectForName(tmp);

                if (root) {
                    String obj = method.getString("/Storage?$filter=IsRemoved eq false", LIST_STORAGE);
                    if (obj != null && obj.length() > 0) {
                        JSONArray json = new JSONArray(obj);
                        for (int i=0; i<json.length(); i++) {
                            JSONObject node = json.getJSONObject(i);
                            Blob container = toBlob(node, "", true, null);
                            if (container != null && container.getBucketName().equalsIgnoreCase(bucketName)) {
                                return  container;
                            }
                        }
                    }
                }

                JSONObject body = new JSONObject();
                body.put("StorageID", storageID);
                body.put("Path", path);
                body.put("Pattern", pattern);
                String obj = method.postString("/Storage/StorageSearchFile", body.toString(),LIST_STORAGE);
                if (obj != null && obj.length()> 0) {
                    JSONObject json = new JSONObject(obj);
                    String response = provider.parseTaskID(json);
                    if (response != null && response.length()> 0) {
                        JSONArray objects = new JSONArray(response);
                        for (int i=0; i<objects.length(); i++) {
                            JSONObject result = objects.getJSONObject(i);
                            boolean isContainer = result.getBoolean("IsDirectory");
                            Blob object = toBlob(result,bucketName,isContainer, storageRegionID);
                            if (object != null) {
                                return object;
                            }
                        }
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
    public Blob getObject(@Nullable String bucketName, @Nonnull String objectName) throws InternalException, CloudException {
        APITrace.begin(provider, GET_OBJECT);
        try {
            if (bucketName == null || objectName == null) {
                return null;
            }
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                int position = bucketName.indexOf("/");
                String tmp;
                String path;
                String pattern = objectName;
                if (position>0) {
                    tmp = bucketName.substring(0, bucketName.indexOf("/"));
                    path = bucketName.substring(bucketName.indexOf("/"));
                    if (path.equals("")) {
                        path = "/";
                    }
                }
                else {
                    tmp = bucketName;
                    path = "/";
                }

                findStorageObjectForName(tmp);

                JSONObject body = new JSONObject();
                body.put("StorageID", storageID);
                body.put("Path", path);
                body.put("Pattern", pattern);
                String obj = method.postString("/Storage/StorageSearchFile", body.toString(),LIST_STORAGE);
                if (obj != null && obj.length()> 0) {
                    JSONObject json = new JSONObject(obj);
                    String response = provider.parseTaskID(json);

                    if (response != null && response.length()> 0) {
                        JSONArray objects = new JSONArray(response);
                        for (int i=0; i<objects.length(); i++) {
                            JSONObject result = objects.getJSONObject(i);
                            boolean isContainer = result.getBoolean("IsDirectory");
                            Blob object = toBlob(result,bucketName,isContainer, storageRegionID);
                            if (object != null && object.getObjectName().equalsIgnoreCase(objectName)) {
                                return object;
                            }
                        }
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

    @Nullable
    @Override
    public Storage<Byte> getObjectSize(@Nullable String bucketName, @Nullable String objectName) throws InternalException, CloudException {
        APITrace.begin(provider, GET_OBJECT_SIZE);
        try {
            Blob blob = getObject(bucketName, objectName);
            if (blob != null) {
                return blob.getSize();
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public int getMaxBuckets() throws CloudException, InternalException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Storage<Byte> getMaxObjectSize() throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMaxObjectsPerBucket() throws CloudException, InternalException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull NameRules getBucketNameRules() throws CloudException, InternalException {
        //return NameRules.getInstance(minChars, maxChars, mixedCase, allowNumbers, latin1Only, specialChars);
        return NameRules.getInstance(1, 255, true, true, true, new char[] { '-', '.' });
    }

    @Override
    public @Nonnull NameRules getObjectNameRules() throws CloudException, InternalException {
        return NameRules.getInstance(1, 255, true, true, true, new char[] { '-', '.', ',', '#', '+' });
    }

    @Nonnull
    @Override
    public String getProviderTermForBucket(@Nonnull Locale locale) {
        return "Storage";
    }

    @Nonnull
    @Override
    public String getProviderTermForObject(@Nonnull Locale locale) {
        return "File";
    }

    @Override
    public boolean isPublic(@Nullable String bucket, @Nullable String object) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, IS_SUBSCRIBED);
        try {
            VirtustreamMethod method = new VirtustreamMethod(provider);
            method.getString("/Storage?$filter=IsRemoved eq false", LIST_STORAGE);
            return true;
        }
        catch (Throwable ignore) {
            return false;
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public Iterable<Blob> list(@Nullable String bucket) throws CloudException, InternalException {
        APITrace.begin(provider, LIST_STORAGE);
        try {
            try {
                ArrayList<Blob> list = new ArrayList<Blob>();
                VirtustreamMethod method = new VirtustreamMethod(provider);
                if (bucket == null) {
                    String obj = method.getString("/Storage?$filter=IsRemoved eq false", LIST_STORAGE);
                    if (obj != null && obj.length() > 0) {
                        JSONArray json = new JSONArray(obj);
                        for (int i=0; i<json.length(); i++) {
                            JSONObject node = json.getJSONObject(i);
                            Blob container = toBlob(node, "", true, null);
                            if (container != null) {
                                list.add(container);
                            }
                        }
                    }
                }
                else {
                    int position = bucket.indexOf("/");
                    String tmp;
                    String path;
                    if (position>0) {
                        tmp = bucket.substring(0, bucket.indexOf("/"));
                        path = bucket.substring(bucket.indexOf("/"));
                    }
                    else {
                        tmp = bucket;
                        path = "/";
                    }

                    findStorageObjectForName(tmp);

                    JSONObject body = new JSONObject();
                    body.put("StorageID", storageID);
                    body.put("Path", path);
                    body.put("Pattern", "*");
                    String obj = method.postString("/Storage/StorageSearchFile", body.toString(),LIST_STORAGE);
                    if (obj != null && obj.length()> 0) {
                        JSONObject json = new JSONObject(obj);
                        String response = provider.parseTaskID(json);
                        if (response != null && response.length()> 0) {
                            JSONArray objects = new JSONArray(response);
                            for (int i=0; i<objects.length(); i++) {
                                JSONObject result = objects.getJSONObject(i);
                                boolean isContainer = result.getBoolean("IsDirectory");
                                Blob object = toBlob(result,bucket,isContainer, storageRegionID);
                                if (object != null) {
                                    list.add(object);
                                }
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
    public void makePublic(@Nonnull String bucket) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Virtustream does not support bucket sharing");
    }

    @Override
    public void makePublic(@Nullable String bucket, @Nonnull String object) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Virtustream does not support bucket sharing");
    }

    @Override
    public void move(@Nullable String fromBucket, @Nullable String objectName, @Nullable String toBucket) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Virtustream does not support object moving");
    }

    @Override
    public void removeBucket(@Nonnull String bucket) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Virtustream does not support deleting buckets");
    }

    @Override
    public void removeObject(@Nullable String bucket, @Nonnull String object) throws CloudException, InternalException {
        APITrace.begin(provider, REMOVE_OBJECT);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                String storageName = bucket;
                String path = "/";
                if (bucket.indexOf("/") >=0) {
                    storageName = bucket.substring(0, bucket.indexOf("/"));
                    path = bucket.substring(bucket.indexOf("/"));
                }
                findStorageObjectForName(storageName);

                JSONObject json = new JSONObject();
                json.put("StorageID", storageID);
                json.put("FilePath", path+"/"+object);

                String obj = method.postString("/Storage/DeleteFile", json.toString(), REMOVE_OBJECT);
                if (obj != null && obj.length() > 0) {
                    JSONObject response = new JSONObject(obj);
                    if (provider.parseTaskID(response) == null) {
                        logger.warn("No confirmation of RemoveObject task completion but no error either");
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

    @Nonnull
    @Override
    public String renameBucket(@Nonnull String oldName, @Nonnull String newName, boolean findFreeName) throws CloudException, InternalException {
        APITrace.begin(provider, RENAME_BUCKET);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                if (oldName.indexOf("/") >=0) {
                    // can only rename the top level storage not lower folders
                    throw new OperationNotSupportedException("Virtustream does not support rename of folders");
                }
                findStorageObjectForName(oldName);

                JSONObject json = new JSONObject();
                json.put("StorageID", storageID);
                json.put("CustomerDefinedName", newName);

                String obj = method.postString("/Storage/RenameStorage", json.toString(), RENAME_BUCKET);
                if (obj != null && obj.length() > 0) {
                    JSONObject response = new JSONObject(obj);
                    if (provider.parseTaskID(response) == null) {
                        logger.warn("No confirmation of RenameBucket task completion but no error either");
                    }
                }

                return newName;
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
    public void renameObject(@Nullable String bucket, @Nonnull String oldName, @Nonnull String newName) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Virtustream does not support object renaming");
    }

    @Nonnull
    @Override
    public Blob upload(@Nonnull File sourceFile, @Nullable String bucket, @Nonnull String objectName) throws CloudException, InternalException {
        APITrace.begin(provider, UPLOAD_FILE);
        try {
            if( bucket == null ) {
                logger.error("No bucket was specified for this request");
                throw new OperationNotSupportedException("No bucket was specified for this request");
            }
            if( !exists(bucket) ) {
                logger.error("Creating new bucket not supported for cloud");
                throw new OperationNotSupportedException("Creating new bucket not supported for cloud");
            }

            put(bucket, objectName, sourceFile);
            return getObject(bucket, objectName);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    protected void get(@Nullable String bucket, @Nonnull String object, @Nonnull File toFile, @Nullable FileTransfer transfer) throws InternalException, CloudException {
        APITrace.begin(provider, DOWNLOAD_FILE);
        try {
            if( bucket == null ) {
                logger.error("No bucket was specified");
                throw new CloudException("No bucket was specified");
            }

            InputStream input = null;

            JSONObject json = null;
            String fileTransferID = null;
            long fileSize = 0;
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                int position = bucket.indexOf("/");
                String tmp;
                String path;
                if (position>0) {
                    tmp = bucket.substring(0, bucket.indexOf("/"));
                    path = bucket.substring(bucket.indexOf("/"));
                    if (path.equals("")) {
                        path = "";
                    }
                }
                else {
                    tmp = bucket;
                    path = "";
                }

                path = path+"/"+object;
                findStorageObjectForName(tmp);

                json = new JSONObject();
                json.put("Command", "BeginDownload");
                json.put("StorageID", storageID);
                json.put("FilePath", path);

                String obj = method.postString("/fileService", json.toString(), DOWNLOAD_FILE);
                if (obj != null && obj.length()>0) {
                    JSONObject response = new JSONObject(obj);
                    JSONObject ft = response.getJSONObject("FileTransfer");
                    fileTransferID = ft.getString("FileTransferID");
                    fileSize=ft.getLong("FileSizeBytes");
                    if (provider.parseStorageTaskID(response) == null) {
                        logger.error("No confirmation of DownloadFile task completion but no error either");
                    }
                }


                if (fileTransferID != null) {
                    input = getBlocks(fileTransferID, fileSize);
                    try {
                        method.postString("/fileService/"+fileTransferID+"/CompleteDownload", "", DOWNLOAD_FILE);
                    }
                    catch (Throwable ex) {
                        logger.error(ex);
                        //cancel upload
                        method.postString("/fileService/"+fileTransferID+"/CancelDownload", "", CANCEL_DOWNLOAD);
                        throw new CloudException(ex);
                    }
                }
                else {
                    logger.error("Unable to transfer file as initiation failed");
                    throw new InternalException("Unable to transfer file as initiation failed");
                }
            }
            catch (JSONException e) {
                logger.error(e);
                throw new InternalException("Unable to parse JSONObject "+e.getMessage());
            }


            if( input == null ) {
                logger.error("No such file: " + bucket + "/" + object);
                throw new CloudException("No such file: " + bucket + "/" + object);
            }
            try {
                copy(input, new FileOutputStream(toFile), transfer);
            }
            catch( FileNotFoundException e ) {
                logger.error("Could not find target file to fetch to " + toFile + ": " + e.getMessage());
                throw new InternalException(e);
            }
            catch( IOException e ) {
                logger.error("Could not fetch file to " + toFile + ": " + e.getMessage());
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    private InputStream getBlocks(@Nonnull String fileTransferID, @Nonnull long fileSize) throws InternalException, CloudException {
        int basicId = 0;
        int blockSize = 10 * 1024;// * 1024;
        byte[] bytes = new byte[blockSize];
        ByteArrayOutputStream fullBytes = new ByteArrayOutputStream();
        int read;
        InputStream response = null;

        try{
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                response = method.getFileDownload("/fileService/"+fileTransferID+"?Position="+basicId+"&ChunkSize="+blockSize, DOWNLOAD_FILE);
                while ((read = response.read(bytes)) != -1) {
                    basicId = basicId+read;
                    if( read < blockSize ) {
                        byte [] subArray = Arrays.copyOfRange(bytes, 0, read);
                        fullBytes.write(subArray);
                     //   break;
                    }
                    else {
                        fullBytes.write(bytes);
                       //
                    }
                    response = method.getFileDownload("/fileService/"+fileTransferID+"?Position="+basicId+"&ChunkSize="+blockSize, DOWNLOAD_FILE);
                    if (response.available()<=0) {
                        break;
                    }
                }

                response = new ByteArrayInputStream(((ByteArrayOutputStream) fullBytes).toByteArray());
                return response;
            }
            catch( IOException e ) {
                throw new CloudException(e);
            }
        }
        finally{
            try {
                if (response!=null) {
                    response.close();
                }
            }
            catch( Throwable ignore ) { }
        }
    }

    @Override
    protected void put(@Nullable String bucket, @Nonnull String objectName, @Nonnull File file) throws InternalException, CloudException {
        if( bucket == null ) {
            throw new CloudException("No bucket was specified");
        }
        String fileTransferID = null;

        InputStream input;

        try {
            input =  new FileInputStream(file);
        }
        catch( IOException e ) {
            logger.error("Error reading input file " + file + ": " + e.getMessage());
            throw new InternalException(e);
        }

        JSONObject json = null;

        try {
            VirtustreamMethod method = new VirtustreamMethod(provider);
            int position = bucket.indexOf("/");
            String tmp;
            String path;
            if (position>0) {
                tmp = bucket.substring(0, bucket.indexOf("/"));
                path = bucket.substring(bucket.indexOf("/"));
                if (path.equals("")) {
                    path = "";
                }
            }
            else {
                tmp = bucket;
                path = "";
            }
            //path = path.replaceAll("//", "\\");

            findStorageObjectForName(tmp);

            json = new JSONObject();
            json.put("Command", "BeginUpload");
            json.put("StorageID", storageID);
            json.put("FilePath", path+"/"+objectName);
            json.put("FileSizeBytes", file.length());


            String obj = method.postString("/fileService", json.toString(), UPLOAD_FILE);
            if (obj != null && obj.length()>0) {
                JSONObject response = new JSONObject(obj);
                fileTransferID = response.getString("FileTransferID");
            }


            if (fileTransferID != null) {
                putBlocks(fileTransferID, input);
                String response = method.postString("/fileService/"+fileTransferID+"/CompleteUpload", "", UPLOAD_FILE);
                if (response != null && response.length() > 0) {
                    JSONObject node = new JSONObject(response);
                    if (provider.parseStorageTaskID(node) == null) {
                        logger.warn("No confirmation of CompleteUpload task completion but no error either");
                    }
                }
            }
            else {
                logger.error("Unable to transfer file as initiation failed");
                throw new InternalException("Unable to transfer file as initiation failed");
            }
        }
        catch (JSONException e) {
            logger.error(e);
            throw new InternalException("Unable to parse JSONObject "+e.getMessage());
        }
    }

    @Override
    protected void put(@Nullable String bucketName, @Nonnull String objectName, @Nonnull String content) throws InternalException, CloudException {
        APITrace.begin(provider, "Blob.put");
        try {
            try {
                File tmp = File.createTempFile(objectName, ".txt");
                PrintWriter writer;

                try {
                    writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tmp)));
                    writer.print(content);
                    writer.flush();
                    writer.close();
                    put(bucketName, objectName, tmp);
                }
                finally {
                    if( !tmp.delete() ) {
                        logger.warn("Unable to delete temp file: " + tmp);
                    }
                }
            }
            catch( IOException e ) {
                logger.error("Failed to write file: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    private void putBlocks(@Nonnull String fileTransferID, @Nonnull InputStream input) throws  InternalException, CloudException {
        int basicId = 0;
        int blockSize = 10 * 1024;// * 1024;
        byte[] bytes = new byte[blockSize];
        int read;

        try{
            try {
                while ((read = input.read(bytes)) != -1) {
                    if( read < blockSize ) {
                        byte [] subArray = Arrays.copyOfRange(bytes, 0, read);
                        putBlocks(subArray, basicId, fileTransferID);
                    }
                    else {
                        putBlocks(bytes, basicId, fileTransferID);
                    }
                    basicId ++;
                }
            }
            catch( IOException e ) {
                throw new CloudException(e);
            }
        }
        catch (Exception ex) {
            logger.error(ex);
            //cancel upload
            VirtustreamMethod method = new VirtustreamMethod(provider);
            method.postString("/fileService/"+fileTransferID+"/CancelUpload", "", CANCEL_UPLOAD);
            throw new CloudException(ex);
        }
        finally{
            try { input.close(); }
            catch( Throwable ignore ) { }
        }
    }

    private void putBlocks(@Nonnull byte[] content, @Nonnull int blockId, @Nonnull String fileTransferID) throws  InternalException, CloudException {
        try {
            VirtustreamMethod method = new VirtustreamMethod(provider);
            JSONObject json = new JSONObject();
            json.put("Sequence", blockId);
            json.put("Data", content);

            method.postString("/fileService/"+fileTransferID, json.toString(), UPLOAD_FILE);
        }
        catch (JSONException e) {
            logger.error(e);
            throw new InternalException("Unable to parse JSONObject");
        }
    }

    @Nonnull
    @Override
    public String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    private Blob toBlob(@Nonnull JSONObject json, @Nonnull String bucket, @Nonnull boolean isContainer, @Nullable String regionId) throws InternalException, CloudException {
        try {
            String object, path;
            long size = -1L, creationDate = 0L;

            if (bucket == null || bucket.equalsIgnoreCase("")) {
                object = json.getString("Name");
                path = object;
                bucket = object;
                JSONObject hv = json.getJSONObject("Hypervisor");
                JSONObject site = hv.getJSONObject("Site");
                JSONObject r = site.getJSONObject("Region");
                regionId = r.getString("RegionID");
            }
            else {
                object = json.getString("Name");
                path = bucket+"/"+object;
                String lastModified = json.getString("LastModified");
                SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                try {
                    Date d = f.parse(lastModified);
                    long milliseconds = d.getTime();
                    creationDate = milliseconds;
                }
                catch (ParseException e) {
                    logger.error(e);
                }
                size = json.getLong("Size");
            }

            if( isContainer ) {
                return Blob.getInstance(regionId, "/"+path, path, creationDate);
            }
            else {
                return Blob.getInstance(regionId, "/"+path, bucket, object, creationDate, new Storage<Byte>(size, Storage.BYTE));
            }
        }
        catch (JSONException e) {
            logger.error(e);
            throw new InternalException("Unable to parse JSONObject "+e.getMessage());
        }
    }

    transient String storageRegionID;
    transient String storageID;
    private void findStorageObjectForName(@Nonnull String storageName) throws InternalException, CloudException {
        APITrace.begin(provider, FIND_STORAGE_ID);
        try {
            try {
                VirtustreamMethod method = new VirtustreamMethod(provider);
                String obj = method.getString("/Storage?$filter=IsRemoved eq false", FIND_STORAGE_ID);
                if (obj != null && obj.length() > 0) {
                    JSONArray json = new JSONArray(obj);
                    for (int i=0; i<json.length(); i++) {
                        JSONObject node = json.getJSONObject(i);

                        String name = node.getString("Name");
                        String id = node.getString("StorageID");
                        JSONObject hv = node.getJSONObject("Hypervisor");
                        JSONObject site = hv.getJSONObject("Site");
                        JSONObject r = site.getJSONObject("Region");
                        storageRegionID = r.getString("RegionID");
                        if (storageName.equalsIgnoreCase(name)) {
                            storageID = id;
                        }
                    }

                }
                if (storageRegionID == null || storageID == null)  {
                    logger.error("Storage with name "+storageName+" not found");
                    throw new InternalException("Storage with name "+storageName+" not found");
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
}
