package com.linbit.linstor.api;


import com.amazonaws.SdkClientException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.ObsRemote;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.utils.Pair;
import com.obs.services.BasicObsCredentialsProvider;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.exception.ObsException;
import com.obs.services.model.*;
import com.obs.services.model.DeleteObjectsResult.DeleteObjectResult;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Singleton
public class BackupToObs
{
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    private final StltConfigAccessor stltConfigAccessor;
    private final DecryptionHelper decHelper;
    private final ErrorReporter errorReporter;
    /**
     * Caches the .metafile content from obs. cache structure:
     * Map<Remote, Map<ObsKey, Pair<Etag, content of .meta file>>>
     * The content of the .meta file is stored as String and is re-parsed as BackupMetaDataPojo with each request to
     * ensure immutability
     */
    private final HashMap<ObsRemote, HashMap<String, Pair<String, String>>> cache;

    @Inject
    public BackupToObs(
            StltConfigAccessor stltConfigAccessorRef,
            DecryptionHelper decHelperRef,
            ErrorReporter errorReporterRef
    )
    {
        stltConfigAccessor = stltConfigAccessorRef;
        decHelper = decHelperRef;
        errorReporter = errorReporterRef;

        cache = new HashMap<>();
    }

    public String initMultipart(String key, ObsRemote remote, AccessContext accCtx, byte[] masterKey)
        throws AccessDeniedException
    {
        final ObsClient obs= getObsClient(remote, accCtx, masterKey);
        String bucketName = remote.getBucket(accCtx);

        boolean reqPays = getRequesterPays(remote,accCtx,obs,bucketName);

        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName,key);
        request.setRequesterPays(reqPays);

        InitiateMultipartUploadResult result = obs.initiateMultipartUpload(request);

        return result.getUploadId();
    }

    public void putObjectMultipart(
            String key,
            InputStream input,
            long maxSize,
            String uploadId,
            ObsRemote remote,
            AccessContext accCtx,
            byte [] masterKey
    )   throws AccessDeniedException, ObsException, IOException, StorageException
    {
        assert maxSize >= 0;

        final ObsClient obs = getObsClient(remote, accCtx, masterKey);

        String bucketName = remote.getBucket(accCtx);
        boolean reqPays = getRequesterPays(remote, accCtx, obs, bucketName);

        long bufferSize = Math.max(5 << 20, (long)(Math.ceil((maxSize * 1024) / 10000.0) + 1.0));
        if(bufferSize > Integer.MAX_VALUE)
        {
            throw new StorageException(
                    "Can only ship parts up to " + Integer.MAX_VALUE + " bytes." +
                            " Current shipment would require parts with a size of " + bufferSize + " bytes."
            );
        }
        List<PartEtag> parts = new ArrayList<>();

        byte [] readBuf = new byte[(int) bufferSize];
        int offset = 0;
        int partId = 1;
        for(int readLen = input.read(readBuf, offset, readBuf.length-offset);
            readLen != -1;
            readLen = input.read(readBuf,offset, readBuf.length-offset) )
        {
            offset += readLen;
            if(readBuf.length == offset)
            {
                UploadPartRequest uploadPartRequest = new UploadPartRequest(
                        bucketName,
                        key,
                        (long)offset,
                        new ByteArrayInputStream(readBuf)
                        );
                uploadPartRequest.setRequesterPays(reqPays);
                uploadPartRequest.setUploadId(uploadId);
                uploadPartRequest.setPartNumber(partId);

                UploadPartResult uploadPartResult = obs.uploadPart(uploadPartRequest);
                parts.add(new PartEtag(uploadPartResult.getEtag(), partId));

                partId++;
                offset = 0;
            }
        }
        if(offset != 0)
        {
            UploadPartRequest uploadPartRequest = new UploadPartRequest(
                bucketName,
                key,
                (long)offset,
                new ByteArrayInputStream(readBuf, 0, offset)
            );

            uploadPartRequest.setBucketName(bucketName);
            uploadPartRequest.setObjectKey(key);
            uploadPartRequest.setUploadId(uploadId);
            uploadPartRequest.setPartNumber(partId);
            uploadPartRequest.setInput(new ByteArrayInputStream(readBuf));
            uploadPartRequest.setOffset(offset);
            uploadPartRequest.setRequesterPays(reqPays);

            UploadPartResult uploadPartResult = obs.uploadPart(uploadPartRequest);
            parts.add(new PartEtag(uploadPartResult.getEtag(), partId));
        }
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
                bucketName,
                key,
                uploadId,
                parts
        );
        compRequest.setRequesterPays(reqPays);
        obs.completeMultipartUpload(compRequest);
        errorReporter.logTrace("Backup upload of %s to bucket  %s completed in %d parts", key, bucketName, partId);
    }
    public void abortMultipart(String key, String uploadId, ObsRemote remote, AccessContext accCtx, byte[] masterKey)
            throws AccessDeniedException, ObsException
    {
        final ObsClient obs = getObsClient(remote, accCtx, masterKey);

        String bucket = remote.getBucket(accCtx);
        boolean reqPays = getRequesterPays(remote,accCtx,obs,bucket);

        AbortMultipartUploadRequest abortReq = new AbortMultipartUploadRequest(
                bucket,
                key,
                uploadId
        );
        abortReq.setRequesterPays(reqPays);

        obs.abortMultipartUpload(abortReq);
    }
    public void putObject(String key, String content, ObsRemote remote, AccessContext accCtx, byte[] masterKey)
            throws AccessDeniedException, SdkClientException
    {
        final ObsClient obs = getObsClient(remote, accCtx, masterKey);
        String bucket = remote.getBucket(accCtx);
        boolean reqPays = getRequesterPays(remote, accCtx, obs, bucket);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength((long) content.getBytes().length);
        PutObjectRequest req = new PutObjectRequest(bucket, key,new ByteArrayInputStream(content.getBytes()));
        req.setMetadata(meta);
        req.setRequesterPays(reqPays);

        obs.putObject(req);
    }
    public void deleteObjects(Set<String> keys, ObsRemote remote, AccessContext accCtx, byte [] masterKey)
        throws AccessDeniedException
    {
        String bucket = remote.getBucket(accCtx);
        final ObsClient obs = getObsClient(remote, accCtx, masterKey);
        boolean reqPays = getRequesterPays(remote, accCtx, obs, bucket);
        DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket);
        deleteObjectsRequest.setKeyAndVersions(keys.stream().map(key -> new KeyAndVersion(key, "")).toArray(KeyAndVersion[]::new));
        if(remote.isMultiDeleteSupported(accCtx))
        {
            try
            {
                DeleteObjectsResult deletedObjs = obs.deleteObjects(deleteObjectsRequest);
                HashMap<String, Pair<String, String>> remoteCache;
                synchronized (cache)
                {
                    remoteCache = lazyGet(cache, remote);
                }
                synchronized (remoteCache)
                {
                    for(DeleteObjectResult deleted : deletedObjs.getDeletedObjectResults())
                    {
                        remoteCache.remove(deleted.getObjectKey());
                    }
                }
            }
            catch (Exception exc)
            {
                if(exc instanceof ObsException)
                {
                    throw exc;
                }
                errorReporter.logWarning(
                        "Exception occurred while trying multi-object-delete on remote %s. Retrying with multiple single-object-deletes",
                        remote.getName().displayValue
                );
                remote.setMultiDeleteSupported(accCtx, false);
                deleteSingleObjects(keys, bucket, reqPays, obs, remote);
            }
        }
        else
        {
            errorReporter.logDebug(
                    "Multi-object-delete not supported due to prior exception on remote %s. Using multiple single-object-deletes instead",
                    remote.getName().displayValue
            );
            deleteSingleObjects(keys, bucket, reqPays, obs, remote);
        }
    }
    private void deleteSingleObjects(Set<String> keys, String bucket, boolean reqPays, ObsClient obs, ObsRemote remote)
    {
        List<DeleteObjectsResult.DeleteObjectResult> deleted = new ArrayList<>();
        HashMap<String,Pair<String, String>> remoteCache;
        synchronized (cache)
        {
            remoteCache = lazyGet(cache, remote);
        }
        // could use threads, but doesn't seem to be slower this way
        synchronized (remoteCache)
        {
            for (String key : keys)
            {
                try
                {
                    DeleteObjectRequest req = new DeleteObjectRequest(bucket,key);
                    req.setRequesterPays(reqPays);
                    obs.deleteObject(req);
                    remoteCache.remove(key);
                    DeleteObjectResult obj = new DeleteObjectResult(key, null, false,null);
                    deleted.add(obj);
                }
                catch (Exception ignored)
                {
                    // ignored, see below
                }
            }
        }
        if (deleted.size() != keys.size())
        {
            // ignored because caller also ignores these
            throw new ObsException("One or more objects could not be deleted");
        }
    }
    public BackupMetaDataPojo getMetaFile(String key, ObsRemote remote, AccessContext accCtx, byte[] masterKey)
            throws AccessDeniedException, IOException
    {
        String metaFileContent = null;
        if (key.endsWith(".meta"))
        {
            HashMap<String, Pair<String, String>> remoteCache;
            synchronized (cache)
            {
                remoteCache = lazyGet(cache, remote);
            }
            synchronized (remoteCache)
            {
                Pair<String, String> pair = remoteCache.get(key);
                if (pair != null && pair.objB != null)
                {
                    metaFileContent = pair.objB;
                }
            }
        }
        if (metaFileContent == null)
        {
            final ObsClient obs = getObsClient(remote, accCtx, masterKey);

            String bucket = remote.getBucket(accCtx);
            boolean reqPays = getRequesterPays(remote, accCtx, obs, bucket);

            GetObjectRequest req = new GetObjectRequest(bucket, key);
            req.setRequesterPays(reqPays);

            ObsObject obj = obs.getObject(req);
            InputStream obsis = obj.getObjectContent();

            {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                byte[] data = new byte[1024];
                for (int nRead = obsis.read(data, 0, data.length); nRead != -1; nRead = obsis.read(data, 0, data.length))
                {
                    buffer.write(data, 0, nRead);
                }

                buffer.flush();
                byte[] byteArray = buffer.toByteArray();

                metaFileContent = new String(byteArray, StandardCharsets.UTF_8);
            }
            {
                HashMap<String, Pair<String, String>> remoteCache;
                synchronized (cache)
                {
                    remoteCache = lazyGet(cache, remote);
                }
                synchronized (remoteCache)
                {
                    Pair<String, String> pair = remoteCache.get(key);
                    if (pair != null)
                    {
                        pair.objB = metaFileContent;
                    }
                }
            }
        }
        return OBJ_MAPPER.readValue(metaFileContent, BackupMetaDataPojo.class);
    }
    private HashMap<String,Pair<String,String>> lazyGet(
            HashMap<ObsRemote, HashMap<String, Pair<String,String>>> mapRef,
            ObsRemote remote
    )
    {
        HashMap<String, Pair<String,String>> ret = mapRef.get(remote);
        if(ret == null)
        {
            ret = new HashMap<>();
            mapRef.put(remote,ret);
        }
        return ret;
    }
    public void deleteRemoteFromCache(ObsRemote remote)
    {
        synchronized (cache)
        {
            cache.remove(remote);
        }
    }
    public InputStream getObject(String key, ObsRemote remote, AccessContext accCtx, byte [] masterKey)
            throws AccessDeniedException
    {
        final ObsClient obs = getObsClient(remote,accCtx,masterKey);

        String bucket = remote.getBucket(accCtx);
        boolean reqPays = getRequesterPays(remote,accCtx,obs,bucket);

        GetObjectRequest req = new GetObjectRequest(bucket,key);
        req.setRequesterPays(reqPays);
        ObsObject obj = obs.getObject(req);

        return obj.getObjectContent();
    }
    public List<ObsObject> listObjects(String withPrefix, ObsRemote remote, AccessContext accCtx, byte[] masterKey)
            throws AccessDeniedException
    {
        Props backupProps = stltConfigAccessor.getReadonlyProps(ApiConsts.NAMESPC_BACKUP_SHIPPING);
        final ObsClient obs = getObsClient(remote, accCtx, masterKey);
        String bucket = remote.getBucket(accCtx);
        boolean reqPays = getRequesterPays(remote, accCtx, obs, bucket);

        ListObjectsRequest req = new ListObjectsRequest();
        req.setRequesterPays(reqPays);
        req.setBucketName(bucket);
        req.setListTimeout(
                Integer.parseInt(backupProps.getPropWithDefault(ApiConsts.KEY_BACKUP_TIMEOUT, "5")) * 1000
        );

        if(withPrefix != null && withPrefix.length() != 0)
        {
            req.setPrefix(withPrefix);
        }
        ObjectListing objectListing = obs.listObjects(req);
        List<ObsObject> objects= objectListing.getObjects();

        while(objectListing.isTruncated())
        {
            objectListing = obs.listObjects(req);
            objects.addAll(objectListing.getObjects());
        }

        // update local cache
        HashMap<String, Pair<String,String>> remoteCache;
        synchronized (cache)
        {
            remoteCache = lazyGet(cache, remote);
        }
        synchronized (remoteCache)
        {
            HashSet<String> keysToRemoveFromCache = new HashSet<>(remoteCache.keySet());
            for(ObsObject object: objects)
            {
                String key = object.getObjectKey();

                // only cache .meta files, not data files
                if(key.endsWith(".meta"))
                {
                    keysToRemoveFromCache.remove(key);
                    String eTag = object.getMetadata().getEtag();

                    Pair<String, String> pair = remoteCache.get(key);
                    if(pair == null)
                    {
                        pair = new Pair<>(eTag, null);
                        remoteCache.put(key,pair);
                    }
                    else
                    {
                        if(!eTag.equals(pair.objA))
                        {
                            pair.objB = null; //clear the content
                        }
                    }
                }
            }
            for(String keyToRemove : keysToRemoveFromCache)
            {
                remoteCache.remove(keyToRemove);
            }
        }

        return objects;
    }
    private BasicObsCredentialsProvider getCredentials(ObsRemote remote, AccessContext accCtx, byte [] masterKey)
    {
        String accessKey;
        String secretKey;
        try
        {
            accessKey = new String(decHelper.decrypt(masterKey,remote.getAccessKey(accCtx)));
            secretKey = new String(decHelper.decrypt(masterKey,remote.getSecretKey(accCtx)));
        } catch (LinStorException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                            "Decrypting the access key or secret key failed."
                    )
            );
        }
        return new BasicObsCredentialsProvider(accessKey,secretKey);
    }

    private ObsClient getObsClient(ObsRemote remote, AccessContext accCtx, byte[] masterKey) throws AccessDeniedException
    {
        final BasicObsCredentialsProvider obsCreds = getCredentials(remote,accCtx,masterKey);

        final ObsConfiguration endpointConfiguration = new ObsConfiguration();
        endpointConfiguration.setEndPoint(remote.getUrl(accCtx));

        endpointConfiguration.setPathStyle(remote.getFlags().isSet(accCtx, AbsRemote.Flags.OBS_USE_PATH_STYLE));

        return (new ObsClient(obsCreds,endpointConfiguration));
    }
    private boolean getRequesterPays(ObsRemote remote, AccessContext accCtx, ObsClient obs, String bucket) throws AccessDeniedException
    {
        boolean reqPaysSupported = remote.isRequesterPaysSupported(accCtx);
        boolean ret = false;
        if(reqPaysSupported)
        {
            try
            {
                RequestPaymentConfiguration configuration = obs.getBucketRequestPayment(bucket);
                ret = configuration.getPayer() == RequestPaymentEnum.REQUESTER;
            }
            catch (Exception exc)
            {
                remote.setRequesterPaysSupported(accCtx, false);
                errorReporter.logWarning(
                        "Exception occurred while checking for support of requester-pays on remote %s. Defaulting to false",
                        remote.getName().displayValue
                );
            }
        }
        else
        {
            errorReporter.logDebug(
                    "Requester-pays not supported due to prior exception on remote %s.", remote.getName().displayValue
            );
        }
        return ret;
    }
}