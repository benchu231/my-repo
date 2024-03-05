package com.linbit.linstor.core.apicallhandler.controller.backup;

import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.*;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.backups.*;
import com.linbit.linstor.api.pojo.backups.BackupPojo.*;
import com.linbit.linstor.backupshipping.BackupConsts;
import com.linbit.linstor.backupshipping.S3MetafileNameInfo;
import com.linbit.linstor.backupshipping.S3VolumeNameInfo;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupApiHelper.AbsObjectInfo;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupApiHelper.ObsObjectInfo;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupApiHelper.S3ObjectInfo;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apis.BackupApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.*;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.core.objects.remotes.ObsRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfProtectionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.Pair;
import com.obs.services.model.ObsObject;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.linbit.linstor.backupshipping.BackupConsts.META_SUFFIX;

@Singleton
public class CtrlBackupApiCallHandler
{
    private final Provider<AccessContext> peerAccCtx;
    private final AccessContext sysCtx;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ErrorReporter errorReporter;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final BackupToS3 backupHandler;
    private final BackupToObs obsBackupHandler;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteApiCallHandler;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final BackupInfoManager backupInfoMgr;
    private final SystemConfProtectionRepository sysCfgRepo;
    private final ResourceDefinitionRepository rscDfnRepo;
    private final CtrlBackupApiHelper backupHelper;

    @Inject
    public CtrlBackupApiCallHandler(
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @SystemContext AccessContext sysCtxRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ErrorReporter errorReporterRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        BackupToS3 backupHandlerRef,
        BackupToObs obsBackupHandlerRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteApiCallHandlerRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        BackupInfoManager backupInfoMgrRef,
        SystemConfProtectionRepository sysCfgRepoRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        CtrlBackupApiHelper backupHelperRef
    )
    {
        peerAccCtx = peerAccCtxRef;
        sysCtx = sysCtxRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        errorReporter = errorReporterRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        backupHandler = backupHandlerRef;
        obsBackupHandler = obsBackupHandlerRef;
        ctrlSnapDeleteApiCallHandler = ctrlSnapDeleteApiCallHandlerRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        backupInfoMgr = backupInfoMgrRef;
        sysCfgRepo = sysCfgRepoRef;
        rscDfnRepo = rscDfnRepoRef;
        backupHelper = backupHelperRef;
    }

    public Flux<ApiCallRc> deleteBackup(
        String rscName,
        String id,
        String idPrefix,
        String timestamp,
        String nodeName,
        boolean cascading,
        boolean allLocalCluster,
        boolean all,
        String key,
        String remoteName,
        boolean dryRun,
        boolean keepSnaps
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Delete backup",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> deleteBackupInTransaction(
                id,
                idPrefix,
                cascading,
                rscName,
                nodeName,
                timestamp,
                allLocalCluster,
                all,
                key,
                remoteName,
                dryRun,
                keepSnaps
            )
        );
    }

    /**
     * Delete backups from an s3-remote</br>
     * The following combinations are allowed:
     * <dl>
     * <dt>id [cascading]</dt>
     * <dd>delete the backup with this exact name</dd>
     * <dt>idPrefix [cascading]</dt>
     * <dd>delete all backups starting with this name</dd>
     * <dt>key [cascading]</dt>
     * <dd>delete this exact s3-object, whether it is part of a backup or not</dd>
     * <dt>(timestamp|rscName|nodeName)+ [cascading]</dt>
     * <dd>delete all backups fitting the filter:
     * <ul style="list-style: none; margin-bottom: 0px;">
     * <li>timestamp: created before the timestamp</li>
     * <li>rscName: created from this rsc</li>
     * <li>nodeName: uploaded from this node</li>
     * </ul>
     * </dd>
     * <dt>all // force cascading</dt>
     * <dd>delete all backups on the given remote</dd>
     * <dt>allLocalCluster // forced cascading</dt>
     * <dd>delete all backups on the given remote that originated from this cluster</dd>
     * </dl>
     * additionally, all combinations can have these set:
     * <dl>
     * <dt>dryRun</dt>
     * <dd>only prints out what would be deleted</dd>
     * <dt>keepSnaps</dt>
     * <dd>only deletes the backups, no local snapshots</dd>
     * </dl>
     */
    private Flux<ApiCallRc> deleteBackupInTransaction(
        String id,
        String idPrefix,
        boolean cascading,
        String rscName,
        String nodeName,
        String timestamp,
        boolean allLocalCluster,
        boolean all,
        String objKey,
        String remoteName,
        boolean dryRun,
        boolean keepSnaps
    ) throws AccessDeniedException, InvalidNameException
    {
        AbsRemote remote = backupHelper.getRemote(remoteName);
        ToDeleteCollections toDelete = new ToDeleteCollections();

        Map<String, AbsObjectInfo> absLinstorObjects = backupHelper.loadAllLinstorS3OrObsObjects(
            remote,
            toDelete.apiCallRcs
        );
        if (id != null && !id.isEmpty()) // case 1: id [cascading]
        {
            final String delId = id.endsWith(META_SUFFIX) ? id : id + META_SUFFIX;
            deleteByIdPrefix(
                delId,
                false,
                cascading,
                absLinstorObjects,
                remote,
                toDelete
            );
        }
        else
        if (idPrefix != null && !idPrefix.isEmpty()) // case 2: idPrefix [cascading]
        {
            deleteByIdPrefix(
                idPrefix,
                true,
                cascading,
                absLinstorObjects,
                remote,
                toDelete
            );
        }
        else
        if (objKey != null && !objKey.isEmpty()) // case 3: objKey [cascading]
        {
            deleteByObjKey(absLinstorObjects, Collections.singleton(objKey), cascading, toDelete, remote.getType());
            toDelete.objKeys.add(objKey);
            toDelete.objKeysNotFound.remove(objKey); // ignore this
        }
        else
        if (
            timestamp != null && !timestamp.isEmpty() ||
                rscName != null && !rscName.isEmpty() ||
                nodeName != null && !nodeName.isEmpty()
        ) // case 4: (time|rsc|node)+ [cascading]
        {
            deleteByTimeRscNode(
                absLinstorObjects,
                timestamp,
                rscName,
                nodeName,
                cascading,
                toDelete,
                remote.getType()
            );
        }
        else
        if (all) // case 5: all // force cascading
        {
            deleteByObjKey(
                absLinstorObjects,
                absLinstorObjects.keySet(),
                true,
                toDelete,
                remote.getType()
            );
        }
        else
        if (allLocalCluster) // case 6: allCluster // forced cascading
        {
            deleteAllLocalCluster(
                absLinstorObjects,
                toDelete,
                remote.getType()
            );
        }

        Flux<ApiCallRc> deleteSnapFlux = Flux.empty();
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        if (keepSnaps)
        {
            toDelete.snapKeys.clear();
        }

        Flux<ApiCallRc> flux = null;
        if (dryRun)
        {
            boolean nothingToDelete = true;
            if (!toDelete.objKeys.isEmpty())
            {
                StringBuilder sb = new StringBuilder("Would delete s3/obs objects:\n");
                nothingToDelete = false;
                for (String objKeyToDelete : toDelete.objKeys)
                {
                    sb.append("  ").append(objKeyToDelete).append("\n");
                }
                apiCallRc.addEntry(sb.toString(), 0); // retCode 0 as nothing actually happened..
            }
            if (!toDelete.snapKeys.isEmpty())
            {
                nothingToDelete = false;
                StringBuilder sb = new StringBuilder("Would delete Snapshots:\n");
                for (SnapshotDefinition.Key snapKey : toDelete.snapKeys)
                {
                    sb.append("  Resource: ").append(snapKey.getResourceName().displayValue).append(", Snapshot: ")
                        .append(snapKey.getSnapshotName().displayValue).append("\n");
                }
                apiCallRc.addEntry(sb.toString(), 0); // retCode 0 as nothing actually happened..
            }
            if (nothingToDelete)
            {
                // retCode 0 as nothing actually happened..
                apiCallRc.addEntry("Dryrun mode. Although nothing selected for deletion", 0);
            }
        }
        else
        {
            for (SnapshotDefinition.Key snapKey : toDelete.snapKeys)
            {
                deleteSnapFlux = deleteSnapFlux.concatWith(
                    ctrlSnapDeleteApiCallHandler.deleteSnapshot(
                        snapKey.getResourceName().displayValue,
                        snapKey.getSnapshotName().displayValue,
                        null
                    )
                );
            }
            try
            {
                if (!toDelete.objKeys.isEmpty())
                {
                    if (remote.getType().equals(RemoteType.OBS))
                    {
                        obsBackupHandler
                            .deleteObjects(toDelete.objKeys, (ObsRemote) remote, peerAccCtx.get(), backupHelper.getLocalMasterKey());
                    }
                    else if (remote.getType().equals(RemoteType.S3))
                    {
                        backupHandler
                            .deleteObjects(toDelete.objKeys, (S3Remote) remote, peerAccCtx.get(), backupHelper.getLocalMasterKey());
                    }

                }
                else
                {
                    apiCallRc.addEntry(
                        "Could not find any backups to delete.",
                        ApiConsts.FAIL_INVLD_REQUEST | ApiConsts.MASK_BACKUP
                    );
                    flux = Flux.just(apiCallRc);
                }
            }
            catch (MultiObjectDeleteException exc)
            {
                Set<String> deletedKeys = new TreeSet<>();
                for (DeletedObject obj : exc.getDeletedObjects())
                {
                    deletedKeys.add(obj.getKey());
                }
                toDelete.objKeys.removeAll(deletedKeys);
                apiCallRc.addEntry(
                    "Could not delete " + toDelete.objKeys.toString(),
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP
                );
                toDelete.objKeys = deletedKeys;
            }
            apiCallRc.addEntry(
                "Successfully deleted " + toDelete.objKeys.toString(),
                ApiConsts.MASK_SUCCESS | ApiConsts.MASK_BACKUP
            );
        }
        if (!toDelete.objKeysNotFound.isEmpty())
        {
            StringBuilder sb = new StringBuilder("The following S3/OBS keys were not found in the given remote:\n");
            for (String objKeyNotFound : toDelete.objKeysNotFound)
            {
                sb.append("  ").append(objKeyNotFound).append("\n");
            }
            apiCallRc.addEntry(sb.toString(), ApiConsts.WARN_NOT_FOUND);
        }

        apiCallRc.addEntries(toDelete.apiCallRcs);
        if (flux == null)
        {
            flux = Flux.<ApiCallRc>just(apiCallRc).concatWith(deleteSnapFlux);
        }
        return flux;
    }

    /**
     * Finds all keys that match the given idPrefix. If allowMultiSelection is false, makes sure that there is only
     * one match. Afterwards, calls deleteByObjKey with the key(s) that were a match.
     */
    private void deleteByIdPrefix(
        String idPrefixRef,
        boolean allowMultiSelectionRef,
        boolean cascadingRef,
        Map<String, AbsObjectInfo> absLinstorObjects,
        AbsRemote remoteRef,
        ToDeleteCollections toDeleteRef
    )
    {
        TreeSet<String> matchingObjKeys = new TreeSet<>();
        for (String objKey : absLinstorObjects.keySet())
        {
            if (objKey.startsWith(idPrefixRef))
            {
                matchingObjKeys.add(objKey);
            }
        }
        int objKeyCount = matchingObjKeys.size();
        if (objKeyCount == 0)
        {
            toDeleteRef.apiCallRcs.addEntry(
                "No backup with id " + (allowMultiSelectionRef ? "prefix " : "") + "'" + idPrefixRef +
                    "' found on remote '" +
                    remoteRef.getName().displayValue + "'",
                ApiConsts.WARN_NOT_FOUND
            );
        }
        else
        {
            if (objKeyCount > 1 && !allowMultiSelectionRef)
            {
                StringBuilder sb = new StringBuilder("Ambiguous id '");
                sb.append(idPrefixRef).append("' for remote '").append(remoteRef.getName().displayValue)
                    .append("':\n");
                for (String objKey : matchingObjKeys)
                {
                    sb.append("  ").append(objKey).append("\n");
                }
                toDeleteRef.apiCallRcs.addEntry(
                    sb.toString(),
                    ApiConsts.FAIL_NOT_FOUND_BACKUP
                );
            }
            else
            {
                deleteByObjKey(absLinstorObjects, matchingObjKeys, cascadingRef, toDeleteRef, remoteRef.getType());
            }
        }
    }

    private void deleteByObjKey(
        Map<String, AbsObjectInfo> absLinstorObjects,
        Set<String> absKeysToDeleteRef,
        boolean cascadingRef,
        ToDeleteCollections toDeleteRef,
        RemoteType remoteType
    )
    {
        if (remoteType.equals(RemoteType.OBS))
        {
            deleteByObsKey(absLinstorObjects, absKeysToDeleteRef, cascadingRef, toDeleteRef);
        }
        else if (remoteType.equals(RemoteType.S3))
        {
            deleteByS3Key(absLinstorObjects, absKeysToDeleteRef, cascadingRef, toDeleteRef);
        }
    }

    /**
     * Checks whether the given keysToDelete exist, then calls addToDeleteList for those that do.
     */
    private void deleteByS3Key(
        Map<String, AbsObjectInfo> s3LinstorObjects,
        Set<String> s3KeysToDeleteRef,
        boolean cascadingRef,
        ToDeleteCollections toDeleteRef
    )
    {
        for (String s3Key : s3KeysToDeleteRef)
        {
            S3ObjectInfo s3ObjectInfo = (S3ObjectInfo) s3LinstorObjects.get(s3Key);
            if (s3ObjectInfo != null && s3ObjectInfo.doesExist())
            {
                addToDeleteList(s3LinstorObjects, s3ObjectInfo, cascadingRef, toDeleteRef);
            }
            else
            {
                toDeleteRef.objKeysNotFound.add(s3Key);
            }
        }
    }

    /**
     * Checks whether the given keysToDelete exist, then calls addToDeleteList for those that do.
     */
    private void deleteByObsKey(
        Map<String, AbsObjectInfo> obsLinstorObjects,
        Set<String> obsKeysToDeleteRef,
        boolean cascadingRef,
        ToDeleteCollections toDeleteRef
    )
    {
        for (String obsKey : obsKeysToDeleteRef)
        {
            ObsObjectInfo obsObjectInfo = (ObsObjectInfo) obsLinstorObjects.get(obsKey);
            if (obsObjectInfo != null && obsObjectInfo.doesExist())
            {
                addToDeleteList(obsLinstorObjects, obsObjectInfo, cascadingRef, toDeleteRef);
            }
            else
            {
                toDeleteRef.objKeysNotFound.add(obsKey);
            }
        }
    }

    /**
     * Make sure all child-objects get marked for deletion as well, and throw an error if there are child-objects
     * but cascading is false. Also mark all related snapDfns for deletion.
     */
    private static void addToDeleteList(
        Map<String, AbsObjectInfo> objMap,
        AbsObjectInfo absObjectInfo,
        boolean cascading,
        ToDeleteCollections toDelete
    )
    {
        if (absObjectInfo.isMetaFile())
        {
            toDelete.objKeys.add(absObjectInfo.getKey());
            for (AbsObjectInfo childObj : absObjectInfo.getReferences())
            {
                if (childObj.doesExist())
                {
                    if (!childObj.isMetaFile())
                    {
                        toDelete.objKeys.add(childObj.getKey());
                    }
                    // we do not want to cascade upwards. only delete child / data keys
                }
                else
                {
                    toDelete.objKeysNotFound.add(childObj.getKey());
                }
            }
            for (AbsObjectInfo childObj : absObjectInfo.getReferencedBy())
            {
                if (childObj.doesExist())
                {
                    if (childObj.isMetaFile())
                    {
                        if (cascading)
                        {
                            addToDeleteList(objMap, childObj, cascading, toDelete);
                        }
                        else
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl.simpleEntry(
                                    ApiConsts.FAIL_DEPENDEND_BACKUP,
                                    absObjectInfo.getKey() + " should be deleted, but at least " +
                                        childObj.getKey() +
                                        " is referencing it. Use --cascading to delete recursively"
                                )
                            );
                        }
                    }
                    // we should not be referenced by something other than a metaFile
                }
                else
                {
                    toDelete.objKeysNotFound.add(childObj.getKey());
                }
            }
            SnapshotDefinition snapDfn = absObjectInfo.getSnapDfn();
            if (snapDfn != null)
            {
                toDelete.snapKeys.add(snapDfn.getSnapDfnKey());
            }
        }
    }

    /**
     * Find all meta-files that conform to the given filters (timestamp, rscName, nodeName), then call
     * deleteByObjKey with that list.
     */
    private void deleteByTimeRscNode(
        Map<String, AbsObjectInfo> absLinstorObjectsRef,
        String timestampRef,
        String rscNameRef,
        String nodeNameRef,
        boolean cascadingRef,
        ToDeleteCollections toDeleteRef,
        RemoteType remoteTypeRef
    )
    {
        Predicate<String> nodeNameCheck = nodeNameRef == null ||
            nodeNameRef.isEmpty() ? ignore -> true : nodeNameRef::equalsIgnoreCase;
        Predicate<String> rscNameCheck = rscNameRef == null ||
            rscNameRef.isEmpty() ? ignore -> true : rscNameRef::equalsIgnoreCase;
        Predicate<Long> timestampCheck;
        if (timestampRef == null || timestampRef.isEmpty())
        {
            timestampCheck = ignore -> true;
        }
        else
        {
            try
            {
                Date date = BackupConsts.parse(timestampRef);
                timestampCheck = timestamp -> date.after(new Date(timestamp));
            }
            catch (ParseException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_TIME_PARAM,
                        "Failed to parse '" + timestampRef +
                            "'. Expected format: YYYYMMDD_HHMMSS (e.g. 20210824_072543)"
                    ),
                    exc
                );
            }
        }
        TreeSet<String> objKeysToDelete = new TreeSet<>();
        for (AbsObjectInfo absObj : absLinstorObjectsRef.values())
        {
            if (absObj.isMetaFile())
            {
                String objKey = absObj.getKey();
                BackupMetaDataPojo metaFile = absObj.getMetaFile();
                String node = metaFile.getNodeName();
                String rsc = metaFile.getRscName();
                long startTimestamp;
                try
                {
                    AbsMetafileNameInfo meta = null;
                    if (remoteTypeRef.equals(RemoteType.OBS))
                    {
                        meta = new ObsMetafileNameInfo(objKey);
                    }
                    else if (remoteTypeRef.equals(RemoteType.S3))
                    {
                        meta = new S3MetafileNameInfo(objKey);
                    }
                    startTimestamp = meta.backupTime.getTime();
                }
                catch (ParseException exc)
                {
                    throw new ImplementationError("Invalid meta file name", exc);
                }
                if (nodeNameCheck.test(node) && rscNameCheck.test(rsc) && timestampCheck.test(startTimestamp))
                {
                    objKeysToDelete.add(objKey);
                }
            }
        }
        deleteByObjKey(absLinstorObjectsRef, objKeysToDelete, cascadingRef, toDeleteRef, remoteTypeRef);
    }

    /**
     * Find all meta-files that were created by the local cluster, then call deleteByS3Key with that list.
     */
    private void deleteAllLocalCluster(
        Map<String, AbsObjectInfo> absLinstorObjectsRef,
        ToDeleteCollections toDeleteRef,
        RemoteType remoteTypeRef
    )
        throws InvalidKeyException, AccessDeniedException
    {
        String localClusterId = sysCfgRepo.getCtrlConfForView(peerAccCtx.get()).getProp(LinStor.PROP_KEY_CLUSTER_ID);
        Set<String> objKeysToDelete = new TreeSet<>();
        for (AbsObjectInfo absObj : absLinstorObjectsRef.values())
        {
            BackupMetaDataPojo metaFile = absObj.getMetaFile();
            if (metaFile != null && localClusterId.equals(metaFile.getClusterId()))
            {
                objKeysToDelete.add(absObj.getKey());
            }
        }
        deleteByObjKey(absLinstorObjectsRef, objKeysToDelete, true, toDeleteRef, remoteTypeRef);
    }

    /**
     * @return
     * <code>Pair.objA</code>: Map of key -> backupApi <br />
     * <code>Pair.objB</code>: Set of keys that either were not created by linstor or cannot be recognized as such
     * anymore
     */
    public Pair<Map<String, BackupApi>, Set<String>> listBackups(
            String rscNameRef,
            String snapNameRef,
            String remoteNameRef
    )
            throws AccessDeniedException, InvalidNameException
    {
        AccessContext peerCtx = peerAccCtx.get();
        AbsRemote remote = backupHelper.getRemote(remoteNameRef);

        // get ALL keys of the given bucket, including possibly not linstor related ones
        Set<String> keys = backupHelper.getAllKeys(remote, rscNameRef);

        Map<String, BackupApi> retIdToBackupsApiMap = new TreeMap<>();

        /*
         * helper map. If we have "full", "inc1" (based on "full"), "inc2" (based on "inc1"), "inc3" (also based on
         * "full", i.e. if user deleted local inc1+inc2 before creating inc3)
         * This map will look like follows:
         * "" -> [full]
         * "full" -> [inc1, inc3]
         * "inc1" -> [inc2]
         * "" is a special id for full backups
         */
        Map<String, List<BackupApi>> idToUsedByBackupApiMap = new TreeMap<>();

        Set<String> linstorBackupsKeys = new TreeSet<>();

        // add all backups to the list that have useable metadata-files
        for (String key : keys)
        {
            try
            {
                AbsMetafileNameInfo info = remote instanceof ObsRemote ? new ObsMetafileNameInfo(key) : new S3MetafileNameInfo(key);
                if (snapNameRef != null && !snapNameRef.isEmpty() && !snapNameRef.equalsIgnoreCase(info.snapName))
                {
                    // Doesn't match the requested snapshot name, skip it.
                    continue;
                }
                Pair<BackupApi, Set<String>> result = getBackupFromMetadata(peerCtx, key, info, remote, keys);
                BackupApi back = result.objA;
                retIdToBackupsApiMap.put(back.getId(), back);
                linstorBackupsKeys.add(key);
                linstorBackupsKeys.addAll(result.objB);
                String base = back.getBasedOnId();
                if (base != null && !base.isEmpty())
                {
                    List<BackupApi> usedByList = idToUsedByBackupApiMap
                            .computeIfAbsent(base, s -> new ArrayList<>());
                    usedByList.add(back);
                }
            }
            catch (MismatchedInputException exc)
            {
                errorReporter.logWarning(
                        "Could not parse metafile %s. Possibly created with older Linstor version",
                        key
                );
            }
            catch (IOException exc)
            {
                errorReporter.reportError(exc, peerCtx, null, "used s3/obs key: " + key);
            }
            catch (ParseException ignored)
            {
                // Ignored, wrong S3/OBS key format
            }
        }
        keys.removeAll(linstorBackupsKeys);
        linstorBackupsKeys.clear();

        // add all backups to the list that look like backups, and maybe even have a rscDfn/snapDfn, but are not in a
        // metadata-file
        for (String key : keys)
        {
            if (!linstorBackupsKeys.contains(key))
            {
                try
                {
                    AbsVolumeNameInfo info = remote instanceof ObsRemote ? new ObsVolumeNameInfo(key) : new S3VolumeNameInfo(key);
                    if (snapNameRef != null && !snapNameRef.isEmpty() && !snapNameRef.equalsIgnoreCase(info.snapName))
                    {
                        // Doesn't match the requested snapshot name, skip it.
                        continue;
                    }
                    SnapshotDefinition snapDfn = backupHelper.loadSnapDfnIfExists(info.rscName, info.snapName);

                    BackupApi back = getBackupFromVolumeKey(info, keys, linstorBackupsKeys, snapDfn);

                    retIdToBackupsApiMap.put(key, back);
                    linstorBackupsKeys.add(key);
                }
                catch (ParseException ignore)
                {
                    // ignored, not a volume file
                }
            }
        }
        // also check local snapDfns if anything is being uploaded but not yet visible in the s3/obs list (an upload might
        // only be shown in the list when it is completed)
        for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView(peerCtx).values())
        {
            if (
                    rscNameRef != null && !rscNameRef.isEmpty() &&
                            rscDfn.getName().displayValue.equalsIgnoreCase(rscNameRef)
            )
            {
                // Doesn't match the given rsc name, skip it.
                continue;
            }
            // only check in-progress snapDfns
            for (SnapshotDefinition snapDfn : backupHelper.getInProgressBackups(rscDfn))
            {
                String rscName = rscDfn.getName().displayValue;
                String snapName = snapDfn.getName().displayValue;

                if (snapNameRef != null && !snapNameRef.isEmpty() && snapNameRef.equalsIgnoreCase(snapName))
                {
                    // Doesn't match the requested snapshot name, skip it.
                    continue;
                }

                String suffix = snapDfn.getProps(peerCtx).getProp(
                    remote instanceof ObsRemote ? ApiConsts.KEY_BACKUP_OBS_SUFFIX : ApiConsts.KEY_BACKUP_S3_SUFFIX,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                );

                String backupTimeRaw = snapDfn.getProps(peerCtx)
                        .getProp(InternalApiConsts.KEY_BACKUP_START_TIMESTAMP, ApiConsts.NAMESPC_BACKUP_SHIPPING);

//                String[] msgs = {
//                    "API_APPLY_REMOTE:" + snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.API_APPLY_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING) + "\n",
//                    "API_REQUEST_REMOTE:" + snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.API_REQUEST_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING) + "\n",
//                    "KEY_BACKUP_TARGET_REMOTE:" + snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.KEY_BACKUP_TARGET_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING) + "\n",
//                    "KEY_BACKUP_SRC_REMOTE:" + snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.KEY_BACKUP_SRC_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING) + "\n",
//                    "API_APPLY_DELETED_REMOTE:" + snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.API_APPLY_DELETED_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING) + "\n",
//                    "NAMESPC_REMOTE:" + snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.NAMESPC_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING) + "\n",
//                    "API_CHANGED_REMOTE:" + snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.API_CHANGED_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING) + "\n",
//                    snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.API_APPLY_REMOTE, ApiConsts.KEY_REMOTE) + "\n",
//                    snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.API_REQUEST_REMOTE, ApiConsts.KEY_REMOTE) + "\n",
//                    snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.KEY_BACKUP_TARGET_REMOTE, ApiConsts.KEY_REMOTE) + "\n",
//                    snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.KEY_BACKUP_SRC_REMOTE, ApiConsts.KEY_REMOTE) + "\n",
//                    snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.API_APPLY_DELETED_REMOTE, ApiConsts.KEY_REMOTE) + "\n",
//                    snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.NAMESPC_REMOTE, ApiConsts.KEY_REMOTE) + "\n",
//                    snapDfn.getProps(peerCtx)
//                        .getProp(InternalApiConsts.API_CHANGED_REMOTE, ApiConsts.KEY_REMOTE),
//                };
                String targetRemote = snapDfn.getProps(peerCtx)
                        .getProp(InternalApiConsts.KEY_BACKUP_TARGET_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING);

                Date backupTime = new Date(Long.parseLong(backupTimeRaw));

                AbsVolumeNameInfo firstFutureInfo = null;

                Set<String> futureKeys = new TreeSet<>();
                for (SnapshotVolumeDefinition snapVlmDfn : snapDfn.getAllSnapshotVolumeDefinitions(peerCtx))
                {
                    AbsVolumeNameInfo futureInfo = null;
                    if (remote instanceof ObsRemote && remote.getName().displayValue.equals(targetRemote))
                    {
                        futureInfo = new ObsVolumeNameInfo(
                            rscName,
                            "",
                            snapVlmDfn.getVolumeNumber().value,
                            backupTime,
                            suffix,
                            snapName
                        );
                    }
                    else
                    if (remote instanceof S3Remote && remote.getName().displayValue.equals(targetRemote))
                    {
                        futureInfo = new S3VolumeNameInfo(
                            rscName,
                            "",
                            snapVlmDfn.getVolumeNumber().value,
                            backupTime,
                            suffix,
                            snapName
                        );
                    }

                    if (firstFutureInfo == null)
                    {
                        firstFutureInfo = futureInfo;
                    }
                    
                    if (futureInfo != null)
                    {
                        futureKeys.add(futureInfo.toString());
                    }
                }

                if (firstFutureInfo != null)
                {
                    BackupApi back = getBackupFromVolumeKey(
                        firstFutureInfo,
                        futureKeys,
                        linstorBackupsKeys,
                        snapDfn
                    );

                    retIdToBackupsApiMap.put(firstFutureInfo.toString(), back);
                    linstorBackupsKeys.add(firstFutureInfo.toString());
                }
            }
        }

        keys.removeAll(linstorBackupsKeys);
        return new Pair<>(retIdToBackupsApiMap, keys);
    }

    /**
     * Get all information needed for listBackups from the meta-file
     */
    private Pair<BackupApi, Set<String>> getBackupFromMetadata(
        AccessContext peerCtx,
        String metadataKey,
        AbsMetafileNameInfo info,
        AbsRemote remote,
        Set<String> keys
    )
        throws IOException, AccessDeniedException
    {
        BackupMetaDataPojo metaFile = null;
        BackupS3Pojo s3Pojo = null;
        BackupObsPojo obsPojo = null;
        if (remote instanceof S3Remote)
        {
            metaFile = backupHandler
                    .getMetaFile(metadataKey, (S3Remote) remote, peerCtx, backupHelper.getLocalMasterKey());
            s3Pojo = new BackupS3Pojo(metadataKey);
        }
        else
        if (remote instanceof ObsRemote)
        {
            metaFile = obsBackupHandler
                    .getMetaFile(metadataKey, (ObsRemote) remote, peerCtx, backupHelper.getLocalMasterKey());
            obsPojo = new BackupObsPojo(metadataKey);
        }

        Map<Integer, List<BackupMetaInfoPojo>> metaVlmMap = metaFile.getBackups();
        Map<Integer, BackupVolumePojo> retVlmPojoMap = new TreeMap<>(); // vlmNr, vlmPojo

        Set<String> associatedKeys = new TreeSet<>();
        boolean restorable = true;

        for (Entry<Integer, List<BackupMetaInfoPojo>> entry : metaVlmMap.entrySet())
        {
            Integer metaVlmNr = entry.getKey();
            List<BackupMetaInfoPojo> backVlmInfoList = entry.getValue();
            for (BackupMetaInfoPojo backVlmInfo : backVlmInfoList)
            {
                if (!keys.contains(backVlmInfo.getName()))
                {
                    /*
                     * The metafile is referring to a data-file that is not known in the given bucket
                     */
                    restorable = false;
                }
                else
                {
                    try
                    {
                        ObsVolumeNameInfo volInfo = new ObsVolumeNameInfo(backVlmInfo.getName());
                        if (metaVlmNr == volInfo.vlmNr)
                        {
                            long vlmFinishedTime = backVlmInfo.getFinishedTimestamp();
                            BackupVolumePojo retVlmPojo = new BackupVolumePojo(
                                metaVlmNr,
                                BackupConsts.format(new Date(vlmFinishedTime)),
                                vlmFinishedTime,
                                null,
                                new BackupVlmObsPojo(backVlmInfo.getName())
                            );
                            retVlmPojoMap.put(metaVlmNr, retVlmPojo);
                            associatedKeys.add(backVlmInfo.getName());
                        }
                        else
                        {
                            // meta-file vlmNr index corruption
                            restorable = false;
                        }
                    }
                    catch (ParseException ignored)
                    {
                        // meta-file corrupt
                        // s3Key does not match backup name pattern
                        restorable = false;
                    }
                }
            }
        }

        // get rid of ".meta"
        String id = metadataKey.substring(0, metadataKey.length() - 5);
        String basedOn = metaFile.getBasedOn();

        return new Pair<>(
            new BackupPojo(
                id,
                info.rscName,
                info.snapName,
                BackupConsts.format(new Date(metaFile.getStartTimestamp())),
                metaFile.getStartTimestamp(),
                BackupConsts.format(new Date(metaFile.getFinishTimestamp())),
                metaFile.getFinishTimestamp(),
                metaFile.getNodeName(),
                false,
                true,
                restorable,
                retVlmPojoMap,
                basedOn,
                s3Pojo,
                obsPojo
            ),
            associatedKeys
        );
    }

    /**
     * Get all information possible for listBackups from a volume backup that is missing its meta-file
     */
    private BackupApi getBackupFromVolumeKey(
        AbsVolumeNameInfo info,
        Set<String> keys,
        Set<String> usedKeys,
        SnapshotDefinition snapDfn
    )
    {
        Boolean shipping;
        Boolean success;
        String nodeName = null;
        String id = "";
        BackupVlmS3Pojo s3Pojo = null;
        BackupVlmObsPojo obsPojo = null;
        if (info instanceof ObsVolumeNameInfo) 
        {
            obsPojo = new BackupVlmObsPojo(info.toString());
            id = new ObsMetafileNameInfo(info.rscName, info.backupTime, info.suffix, info.snapName)
                    .toFullBackupId();
        }
        else 
        if (info instanceof S3VolumeNameInfo)
        {
            s3Pojo = new BackupVlmS3Pojo(info.toString());
            id = new S3MetafileNameInfo(info.rscName, info.backupTime, info.suffix, info.snapName)
                    .toFullBackupId();
        }
        Map<Integer, BackupVolumePojo> vlms = new TreeMap<>();

        vlms.put(info.vlmNr, new BackupVolumePojo(info.vlmNr, null, null, s3Pojo, obsPojo));

        try
        {
            AccessContext peerCtx = peerAccCtx.get();

            // get all other matching keys
            // add them to vlms
            // add them to usedKeys
            for (String otherKey : keys)
            {
                if (!usedKeys.contains(otherKey) && !otherKey.equals(info.toString()))
                {
                    try
                    {
                        AbsVolumeNameInfo otherInfo = info instanceof ObsVolumeNameInfo ? new ObsVolumeNameInfo(otherKey) : new S3VolumeNameInfo(otherKey);
                        if (otherInfo.rscName.equals(info.rscName) && otherInfo.backupId.equals(info.backupId))
                        {
                            vlms.put(
                                otherInfo.vlmNr,
                                new BackupVolumePojo(
                                    otherInfo.vlmNr,
                                    null,
                                    null,
                                    otherInfo instanceof S3VolumeNameInfo ? new BackupVlmS3Pojo(otherInfo.toString()) : null,
                                    otherInfo instanceof ObsVolumeNameInfo ? new BackupVlmObsPojo(otherInfo.toString()) : null
                                )
                            );
                            usedKeys.add(otherKey);
                        }
                    }
                    catch (ParseException ignored)
                    {
                        // Not a volume file
                    }

                }
            }

            // Determine backup status based on snapshot definition
            if (snapDfn != null && snapDfn.getFlags().isSet(peerCtx, SnapshotDefinition.Flags.BACKUP))
            {
                String ts = snapDfn.getProps(peerCtx)
                    .getProp(InternalApiConsts.KEY_BACKUP_START_TIMESTAMP, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                if (ts == null || ts.isEmpty())
                {
                    throw new ImplementationError(
                        "Snapshot " + snapDfn.getName().displayValue +
                            " has the BACKUP-flag set, but does not have a required internal property set."
                    );
                }
                boolean isShipping = snapDfn.getFlags().isSet(peerCtx, SnapshotDefinition.Flags.SHIPPING);
                boolean isShipped = snapDfn.getFlags().isSet(peerCtx, SnapshotDefinition.Flags.SHIPPED);
                if (isShipping || isShipped)
                {
                    for (Snapshot snap : snapDfn.getAllSnapshots(peerCtx))
                    {
                        if (snap.getFlags().isSet(peerCtx, Snapshot.Flags.BACKUP_SOURCE))
                        {
                            nodeName = snap.getNodeName().displayValue;
                        }
                    }
                    if (isShipping)
                    {
                        shipping = true;
                        success = null;
                    }
                    else // if isShipped
                    {
                        shipping = false;
                        success = true;
                    }
                }
                else
                {
                    shipping = false;
                    success = false;
                }
            }
            else
            {
                shipping = null;
                success = null;
            }
        }
        catch (AccessDeniedException exc)
        {
            // no access to snapDfn
            shipping = null;
            success = null;
        }

        return new BackupPojo(
            id,
            info.rscName,
            info.snapName,
            BackupConsts.format(info.backupTime),
            info.backupTime.getTime(),
            null,
            null,
            nodeName,
            shipping,
            success,
            false,
            vlms,
            null,
            null,
            null
        );
    }

    public Flux<ApiCallRc> backupAbort(String rscNameRef, boolean restore, boolean create, String remoteNameRef)
    {
        return scopeRunner.fluxInTransactionalScope(
            "abort backup",
            lockGuardFactory.create().read(LockObj.NODES_MAP).write(LockObj.RSC_DFN_MAP).buildDeferred(),
            () -> backupAbortInTransaction(rscNameRef, restore, create, remoteNameRef)
        );
    }

    /**
     * Check if create or restore needs to be aborted if not specified by the parameters, then set SHIPPING_ABORT on all
     * affected snapDfns
     */
    private Flux<ApiCallRc> backupAbortInTransaction(
        String rscNameRef,
        boolean restorePrm,
        boolean createPrm,
        String remoteNameRef
    )
        throws AccessDeniedException, DatabaseException, InvalidNameException
    {
        Flux<ApiCallRc> flux = null;
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
        AbsRemote remote = ctrlApiDataLoader.loadRemote(remoteNameRef, true);
        // immediately remove any queued snapshots
        for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(peerAccCtx.get()))
        {
            backupInfoMgr.deleteFromQueue(snapDfn, remote);
        }
        Set<SnapshotDefinition> snapDfns = backupHelper.getInProgressBackups(rscDfn);
        if (snapDfns.isEmpty())
        {
            flux = Flux.empty();
        }

        boolean restore = restorePrm;
        boolean create = createPrm;
        if (!restore && !create)
        {
            restore = true;
            create = true;
        }

        Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateStlts = Flux.empty();
        List<SnapshotDefinition> snapDfnsToUpdate = new ArrayList<>();
        for (SnapshotDefinition snapDfn : snapDfns)
        {
            Collection<Snapshot> snaps = snapDfn.getAllSnapshots(peerAccCtx.get());
            boolean abort = false;
            for (Snapshot snap : snaps)
            {
                boolean crt = snap.getFlags().isSet(peerAccCtx.get(), Snapshot.Flags.BACKUP_SOURCE) && create;
                boolean rst = snap.getFlags().isSet(peerAccCtx.get(), Snapshot.Flags.BACKUP_TARGET) && restore;
                if (crt && remoteNameRef != null)
                {
                    String remoteName = snap.getProps(peerAccCtx.get())
                        .getProp(InternalApiConsts.KEY_BACKUP_TARGET_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                    crt = backupHelper.hasShippingToRemote(remoteName, remoteNameRef);
                }
                if (rst && remoteNameRef != null)
                {
                    String remoteName = snap.getProps(peerAccCtx.get())
                        .getProp(InternalApiConsts.KEY_BACKUP_SRC_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                    rst = backupHelper.hasShippingToRemote(remoteName, remoteNameRef);
                }
                if (crt || rst)
                {
                    abort = true;
                    break;
                }
            }
            if (abort)
            {
                snapDfn.getFlags().disableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
                snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING_ABORT);
                snapDfnsToUpdate.add(snapDfn);
            }
        }

        ctrlTransactionHelper.commit();
        for (SnapshotDefinition snapDfn : snapDfnsToUpdate)
        {
            updateStlts = updateStlts.concatWith(
                ctrlSatelliteUpdateCaller.updateSatellites(snapDfn, CtrlSatelliteUpdateCaller.notConnectedWarn())
            );
        }
        ApiCallRcImpl success = new ApiCallRcImpl();
        success.addEntry(
            "Successfully aborted all " +
                ((create && restore) ?
                    "in-progress backup-shipments and restores" :
                    (create ? "in-progress backup-shipments" : "in-progress backup-restores")) +
                " of resource " + rscNameRef,
            ApiConsts.MASK_SUCCESS
        );
        if (flux == null)
        {
            flux = updateStlts.transform(
                responses -> CtrlResponseUtils.combineResponses(
                    responses,
                    LinstorParsingUtils.asRscName(rscNameRef),
                    "Abort backups of {1} on {0} started"
                )
            ).concatWith(Flux.just(success));
        }
        return flux;
    }

    public Flux<BackupInfoPojo> backupInfo(
        String srcRscName,
        String srcSnapName,
        String backupId,
        Map<String, String> storPoolMapRef,
        String nodeName,
        String remoteName
    )
    {
        Set<NodeName> nodes = Collections.emptySet();
        if (nodeName != null && !nodeName.isEmpty())
        {
            nodes = Collections.singleton(LinstorParsingUtils.asNodeName(nodeName));
        }

        return freeCapacityFetcher
            .fetchThinFreeCapacities(nodes)
            .flatMapMany(
                ignored -> scopeRunner.fluxInTransactionalScope(
                    "restore backup", lockGuardFactory.buildDeferred(
                        LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP
                    ),
                    () -> backupInfoInTransaction(
                        srcRscName,
                        srcSnapName,
                        backupId,
                        storPoolMapRef,
                        nodeName,
                        remoteName
                    )
                )
            );
    }

    /**
     * Find out how if a backup-restore is possible and how much space it would need
     */
    private Flux<BackupInfoPojo> backupInfoInTransaction(
        String srcRscName,
        String srcSnapName,
        String backupId,
        Map<String, String> renameMap,
        String nodeName,
        String remoteName
    ) throws AccessDeniedException, InvalidNameException, IOException {
        AbsRemote remote = backupHelper.getRemote(remoteName);
        AbsMetafileNameInfo metaFile = null;
        byte[] masterKey = backupHelper.getLocalMasterKey();

        List<?> objects = null;
        Set<String> objKeys = null;

        if (backupId != null && !backupId.isEmpty())
        {
            String metaName = backupId;
            if (!metaName.endsWith(META_SUFFIX))
            {
                metaName = backupId + META_SUFFIX;
            }

            try
            {
                if (remote instanceof ObsRemote)
                {
                    metaFile = new ObsMetafileNameInfo(metaName);
                    objects = obsBackupHandler.listObjects(metaFile.rscName, (ObsRemote) remote, peerAccCtx.get(), masterKey);
                    // do not use backupHelper.getAllObsKeys here to avoid two listObjects calls since objects is needed
                    // later
                    objKeys = objects.stream().map(object -> ((ObsObject) object).getObjectKey()).collect(Collectors.toCollection(TreeSet::new));
                }
                else if (remote instanceof S3Remote)
                {
                    metaFile = new S3MetafileNameInfo(metaName);
                    objects = backupHandler.listObjects(metaFile.rscName, (S3Remote) remote, peerAccCtx.get(), masterKey);
                    // do not use backupHelper.getAllS3Keys here to avoid two listObjects calls since objects is needed
                    // later
                    objKeys = objects.stream().map(object -> ((S3ObjectSummary) object).getKey()).collect(Collectors.toCollection(TreeSet::new));
                }
            }
            catch (ParseException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                        "The target backup " + metaName +
                            " is invalid since it does not match the pattern of " +
                            "'<rscName>_back_YYYYMMDD_HHMMSS[optional-backup-obj-suffix][^snapshot-name][.meta]' " +
                            "(e.g. my-rsc_back_20210824_072543)." +
                            "Please provide a valid target backup, or provide only the source resource name " +
                            "to restore to the latest backup of that resource."
                    )
                );
            }
        }
        else
        {
            // No backup was explicitly selected, use the latest available for the source resource.
            if (remote instanceof ObsRemote)
            {
                objects = obsBackupHandler.listObjects(srcRscName, (ObsRemote) remote, peerAccCtx.get(), masterKey);
                // do not use backupHelper.getAllObsKeys here to avoid two listObjects calls since objects is needed later
                objKeys = objects.stream().map(object -> ((ObsObject) object).getObjectKey()).collect(Collectors.toCollection(TreeSet::new));
                metaFile = backupHelper.getLatestBackup(objKeys, srcSnapName, remote.getType());
            }
            else if (remote instanceof S3Remote)
            {
                objects = backupHandler.listObjects(srcRscName, (S3Remote) remote, peerAccCtx.get(), masterKey);
                // do not use backupHelper.getAllS3Keys here to avoid two listObjects calls since objects is needed later
                objKeys = objects.stream().map(object -> ((S3ObjectSummary) object).getKey()).collect(Collectors.toCollection(TreeSet::new));
                metaFile = backupHelper.getLatestBackup(objKeys, srcSnapName, remote.getType());
            }

        }

        if (metaFile == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_BACKUP | ApiConsts.MASK_BACKUP,
                    "Could not find a matching backup for resource '" + srcRscName + "', snapshot '" + srcSnapName +
                        "' and id '" + backupId + "' in remote '" + remoteName + "'"
                )
            );
        }

        if (!objKeys.contains(metaFile.toString()))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN | ApiConsts.MASK_BACKUP,
                    "Could not find the needed meta-file with the name '" + metaFile + "' in remote '" + remoteName +
                        "'"
                )
            );
        }

        String fullBackup = null;
        String latestBackup = metaFile.toFullBackupId();
        String currentMetaName = metaFile.toString();

        LinkedList<BackupMetaDataPojo> data = new LinkedList<>();
        try
        {
            do
            {
                String toCheck = currentMetaName;
                BackupMetaDataPojo metadata = null;
                if (remote instanceof ObsRemote)
                {
                    metadata = obsBackupHandler.getMetaFile(toCheck, (ObsRemote) remote, peerAccCtx.get(), masterKey);
                }
                else if (remote instanceof S3Remote)
                {
                    metadata = backupHandler.getMetaFile(toCheck, (S3Remote) remote, peerAccCtx.get(), masterKey);
                }

                data.add(metadata);
                currentMetaName = metadata.getBasedOn();
                if (currentMetaName == null)
                {
                    fullBackup = toCheck;
                }
            }
            while (currentMetaName != null);
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                    "Failed to parse meta file " + currentMetaName
                )
            );
        }

        long totalDlSizeKib = 0;
        long totalAllocSizeKib = 0;
        Map<StorPoolApi, List<BackupInfoVlmPojo>> storPoolMap = new HashMap<>();
        List<BackupInfoStorPoolPojo> storpools = new ArrayList<>();

        boolean first = true;
        for (BackupMetaDataPojo meta : data)
        {
            Pair<Long, Long> totalSizes = new Pair<>(0L, 0L); // dlSize, allocSize
            fillBackupInfo(first, storPoolMap, objects, meta, meta.getLayerData(), totalSizes, remote.getType());
            first = false;
            totalDlSizeKib += totalSizes.objA;
            totalAllocSizeKib += totalSizes.objB;
        }
        Map<String, Long> remainingFreeSpace = new HashMap<>();

        if (nodeName != null)
        {
            remainingFreeSpace = getRemainingSize(storPoolMap, renameMap, nodeName);
        }
        for (Entry<StorPoolApi, List<BackupInfoVlmPojo>> entry : storPoolMap.entrySet())
        {
            String targetStorPool = renameMap.get(entry.getKey().getStorPoolName());
            if (targetStorPool == null)
            {
                targetStorPool = entry.getKey().getStorPoolName();
            }
            storpools.add(
                new BackupInfoStorPoolPojo(
                    entry.getKey().getStorPoolName(),
                    entry.getKey().getDeviceProviderKind(),
                    targetStorPool,
                    remainingFreeSpace.get(targetStorPool.toUpperCase()),
                    entry.getValue()
                )
            );
        }

        BackupInfoPojo backupInfo = new BackupInfoPojo(
            metaFile.rscName,
            metaFile.snapName,
            fullBackup,
            latestBackup,
            data.size(),
            totalDlSizeKib,
            totalAllocSizeKib,
            storpools
        );

        return Flux.just(backupInfo);
    }

    /**
     * Calculate how much free space would be left over after a restore
     */
    Map<String, Long> getRemainingSize(
        Map<StorPoolApi, List<BackupInfoVlmPojo>> storPoolMap,
        Map<String, String> renameMap,
        String nodeName
    ) throws AccessDeniedException
    {
        Map<String, Long> remainingFreeSpace = new HashMap<>();
        for (Entry<StorPoolApi, List<BackupInfoVlmPojo>> entry : storPoolMap.entrySet())
        {
            String targetStorPool = renameMap.get(entry.getKey().getStorPoolName());
            if (targetStorPool == null)
            {
                targetStorPool = entry.getKey().getStorPoolName();
            }
            long poolAllocSize = 0;
            long poolDlSize = 0;
            StorPool sp = ctrlApiDataLoader.loadStorPool(targetStorPool, nodeName, true);
            Long freeSpace = remainingFreeSpace.get(sp.getName().value);
            if (freeSpace == null)
            {
                freeSpace = sp.getFreeSpaceTracker().getFreeCapacityLastUpdated(sysCtx).orElse(null);
            }
            for (BackupInfoVlmPojo vlm : entry.getValue())
            {
                poolAllocSize += vlm.getAllocSizeKib() != null ? vlm.getAllocSizeKib() : 0;
                poolDlSize += vlm.getDlSizeKib() != null ? vlm.getDlSizeKib() : 0;
            }
            remainingFreeSpace.put(
                sp.getName().value,
                freeSpace != null ? freeSpace - poolAllocSize - poolDlSize : null
            );
        }
        return remainingFreeSpace;
    }

    /**
     * Collect all the info needed for backupInfo
     */
    void fillBackupInfo(
        boolean first,
        Map<StorPoolApi, List<BackupInfoVlmPojo>> storPoolMap,
        List<?> objects,
        BackupMetaDataPojo meta,
        RscLayerDataApi layerData,
        Pair<Long, Long> totalSizes,
        RemoteType remoteType
    ) throws IOException {
        for (RscLayerDataApi child : layerData.getChildren())
        {
            fillBackupInfo(first, storPoolMap, objects, meta, child, totalSizes, remoteType);
        }
        if (layerData.getLayerKind().equals(DeviceLayerKind.STORAGE))
        {
            for (VlmLayerDataApi volume : layerData.getVolumeList())
            {
                if (!storPoolMap.containsKey(volume.getStorPoolApi()))
                {
                    storPoolMap.put(volume.getStorPoolApi(), new ArrayList<>());
                }
                String vlmName = "";
                Long allocSizeKib = null;
                Long useSizeKib = null;
                Long dlSizeKib = null;
                for (BackupMetaInfoPojo backup : meta.getBackups().get(volume.getVlmNr()))
                {
                    try
                    {
                        AbsVolumeNameInfo info = null;
                        if(remoteType.equals(RemoteType.OBS))
                        {
                            info = new ObsVolumeNameInfo(backup.getName());
                        }
                        else if (remoteType.equals(RemoteType.S3))
                        {
                            info = new S3VolumeNameInfo(backup.getName());
                        }
                        if (info.layerSuffix.equals(layerData.getRscNameSuffix()))
                        {
                            vlmName = backup.getName();
                            break;
                        }
                    }
                    catch (ParseException exc)
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                                "A backup name in the meta-file did not match with the backup-name-pattern." +
                                    " The meta-file is either corrupted or created by an outdated version of linstor."
                            )
                        );
                    }
                }
                if (first)
                {
                    allocSizeKib = volume.getSnapshotAllocatedSize();
                    totalSizes.objB += allocSizeKib;
                    useSizeKib = volume.getSnapshotUsableSize();
                }
                for (Object object : objects)
                {
                    if (object instanceof S3ObjectSummary && ((S3ObjectSummary) object).getKey().equals(vlmName))
                    {
                        S3ObjectSummary s3Object = (S3ObjectSummary) object;
                        dlSizeKib = (long) Math.ceil(s3Object.getSize() / 1024.0);
                        totalSizes.objA += dlSizeKib;
                        break;
                    }
                    else if (object instanceof ObsObject && ((ObsObject) object).getObjectKey().equals(vlmName))
                    {
                        ObsObject obsObject = (ObsObject) object;
                        dlSizeKib = (long) Math.ceil(obsObject.getMetadata().getContentLength() / 1024.0);
                        totalSizes.objA += dlSizeKib;
                        break;
                    }
                }
                DeviceLayerKind layerType = RscLayerSuffixes.getLayerKindFromLastSuffix(layerData.getRscNameSuffix());
                BackupInfoVlmPojo vlmPojo = new BackupInfoVlmPojo(
                    vlmName,
                    layerType,
                    dlSizeKib,
                    allocSizeKib,
                    useSizeKib
                );
                storPoolMap.get(volume.getStorPoolApi()).add(vlmPojo);
            }
        }
    }

    private static class ToDeleteCollections
    {
        Set<String> objKeys;
        Set<SnapshotDefinition.Key> snapKeys;
        ApiCallRcImpl apiCallRcs;
        Set<String> objKeysNotFound;

        ToDeleteCollections()
        {
            objKeys = new TreeSet<>();
            snapKeys = new TreeSet<>();
            apiCallRcs = new ApiCallRcImpl();
            objKeysNotFound = new TreeSet<>();
        }
    }
}
