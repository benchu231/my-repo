package com.linbit.linstor.core.apicallhandler.controller.backup;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.*;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.api.pojo.backups.BackupMetaInfoPojo;
import com.linbit.linstor.backupshipping.*;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.remotes.*;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.obs.services.model.ObsObject;
import reactor.core.publisher.Flux;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class CtrlBackupApiHelper
{
    private final CtrlSecurityObjects ctrlSecObj;
    private final RemoteRepository remoteRepo;
    private final Provider<AccessContext> peerAccCtx;
    private final BackupToS3 backupHandler;
    private final BackupToObs obsBackupHandler;
    private final ErrorReporter errorReporter;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final AccessContext sysCtx;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;

    @Inject
    public CtrlBackupApiHelper(
        CtrlSecurityObjects ctrlSecObjRef,
        RemoteRepository remoteRepoRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        BackupToS3 backupHandlerRef,
        BackupToObs obsBackupHandlerRef,
        ErrorReporter errorReporterRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        @SystemContext AccessContext sysCtxRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef
    )
    {
        ctrlSecObj = ctrlSecObjRef;
        remoteRepo = remoteRepoRef;
        peerAccCtx = peerAccCtxRef;
        backupHandler = backupHandlerRef;
        obsBackupHandler = obsBackupHandlerRef;
        errorReporter = errorReporterRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        sysCtx = sysCtxRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;

    }

    /**
     * Check that the masterKey exists and is unlocked, then returns it.
     */
    byte[] getLocalMasterKey()
    {
        byte[] masterKey = ctrlSecObj.getCryptKey();
        if (masterKey == null || masterKey.length == 0)
        {
            throw new ApiRcException(
                ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY,
                        "Unable to decrypt the S3/OBS access key and secret key without having a master key"
                    )
                    .setCause("The masterkey was not initialized yet")
                    .setCorrection("Create or enter the master passphrase")
                    .build()
            );
        }
        return masterKey;
    }

    /**
     * Get the remote with the given name only if it is an s3 remote
     */
    S3Remote getS3Remote(String remoteName) throws AccessDeniedException, InvalidNameException
    {
        AbsRemote remote = getRemote(remoteName);
        if (!(remote instanceof S3Remote))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REMOTE_NAME | ApiConsts.MASK_BACKUP,
                    "The remote " + remoteName + " is not an s3 remote."
                )
            );
        }
        return (S3Remote) remote;
    }

    /**
     * Get the remote with the given name only if it is an obs remote
     */
    ObsRemote getObsRemote(String remoteName) throws AccessDeniedException, InvalidNameException
    {
        AbsRemote remote = getRemote(remoteName);
        if (!(remote instanceof ObsRemote))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REMOTE_NAME | ApiConsts.MASK_BACKUP,
                    "The remote " + remoteName + " is not an obs remote."
                )
            );
        }
        return (ObsRemote) remote;
    }

    /**
     * Get the remote with the given name only if it is a l2l-remote
     */
    LinstorRemote getL2LRemote(String remoteName) throws AccessDeniedException, InvalidNameException
    {
        AbsRemote remote = getRemote(remoteName);
        if (!(remote instanceof LinstorRemote))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REMOTE_NAME | ApiConsts.MASK_BACKUP,
                    "The remote " + remoteName + " is not a linstor remote."
                )
            );
        }
        return (LinstorRemote) remote;
    }

    /**
     * Get the remote with the given name and make sure it exists.
     */
    AbsRemote getRemote(String remoteName) throws AccessDeniedException, InvalidNameException
    {
        if (remoteName == null || remoteName.isEmpty())
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REMOTE_NAME | ApiConsts.MASK_BACKUP,
                    "No remote name was given. Please provide a valid remote name."
                )
            );
        }
        AbsRemote remote = null;
        remote = remoteRepo.get(peerAccCtx.get(), new RemoteName(remoteName, true));
        if (remote == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_REMOTE | ApiConsts.MASK_BACKUP,
                    "The remote " + remoteName + " does not exist. Please provide a valid remote or create a new one."
                )
            );
        }
        return remote;
    }

    /**
     * Get all snapDfns that are currently shipping a backup.
     */
    Set<SnapshotDefinition> getInProgressBackups(ResourceDefinition rscDfn)
        throws AccessDeniedException, InvalidNameException
    {
        return getInProgressBackups(rscDfn, null);
    }

    Set<SnapshotDefinition> getInProgressBackups(ResourceDefinition rscDfn, @Nullable AbsRemote remote)
        throws AccessDeniedException, InvalidNameException
    {
        Set<SnapshotDefinition> snapDfns = new HashSet<>();
        for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(peerAccCtx.get()))
        {
            if (
                snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING) &&
                    snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.BACKUP)
            )
            {
                if (remote == null)
                {
                    snapDfns.add(snapDfn);
                }
                else
                {
                    for (Snapshot snap : snapDfn.getAllSnapshots(peerAccCtx.get()))
                    {
                        String remoteName = "";
                        if (snap.getFlags().isSet(peerAccCtx.get(), Snapshot.Flags.BACKUP_SOURCE))
                        {
                            remoteName = snap.getProps(peerAccCtx.get())
                                .getProp(InternalApiConsts.KEY_BACKUP_TARGET_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                        }
                        else if (snap.getFlags().isSet(peerAccCtx.get(), Snapshot.Flags.BACKUP_TARGET))
                        {
                            remoteName = snap.getProps(peerAccCtx.get())
                                .getProp(InternalApiConsts.KEY_BACKUP_SRC_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                        }

                        if (hasShippingToRemote(remoteName, remote.getName().displayValue))
                        {
                            snapDfns.add(snapDfn);
                            break;
                        }
                    }
                }
            }
        }
        return snapDfns;
    }

    boolean hasShippingToRemote(String remoteToCheck, String expectedRemote)
        throws AccessDeniedException, InvalidNameException
    {
        boolean ret = expectedRemote != null;
        if (ret && !expectedRemote.equalsIgnoreCase(remoteToCheck))
        {
            AbsRemote remote = remoteRepo.get(sysCtx, new RemoteName(remoteToCheck, true));
            if (remote instanceof StltRemote)
            {
                // we checked the stlt-remote instead of the correct remote, check again
                if (!((StltRemote) remote).getLinstorRemoteName().displayValue.equalsIgnoreCase(expectedRemote))
                {
                    // the correct remote doesn't have the same name either
                    ret = false;
                }
                // the correct remote had the same name, ret stays true
            }
            else if (remote instanceof S3Remote)
            {
                // it already is the correct remote, and the name is not the same
                ret = false;
            }
        }
        // the remote has the same name, ret stays true
        return ret;
    }

    /**
     * Get all keys of the given remote, filtered by rscName
     */
    Set<String> getAllKeys(AbsRemote absRemote, String rscName) throws AccessDeniedException
    {
        Set<String> keys = new TreeSet<>();

        if (absRemote instanceof S3Remote)
            keys = getAllS3Keys((S3Remote) absRemote, rscName);
        else if (absRemote instanceof ObsRemote)
            keys = getAllObsKeys((ObsRemote) absRemote, rscName);

        return keys;
    }

    /**
     * Get all s3Keys of the given remote, filtered by rscName
     */
    Set<String> getAllS3Keys(S3Remote s3Remote, String rscName) throws AccessDeniedException
    {
        List<S3ObjectSummary> objects = backupHandler.listObjects(
            rscName,
            s3Remote,
            peerAccCtx.get(),
            getLocalMasterKey()
        );
        // get ALL s3 keys of the given bucket, including possibly not linstor related ones
        return objects.stream()
            .map(S3ObjectSummary::getKey)
            .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Get all obsKeys of the given remote, filtered by rscName
     */
    Set<String> getAllObsKeys(ObsRemote obsRemote, String rscName) throws AccessDeniedException
    {
        List<ObsObject> objects = obsBackupHandler.listObjects(
                rscName,
                obsRemote,
                peerAccCtx.get(),
                getLocalMasterKey()
        );
        // get ALL obs keys of the given bucket, including possibly not linstor related ones
        return objects.stream()
            .map(ObsObject::getObjectKey)
            .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Get all s3/obs objects that can be verified to have been created by linstor
     */
    Map<String, AbsObjectInfo> loadAllLinstorS3OrObsObjects(
        AbsRemote remoteRef,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException
    {
        Set<String> allKeys = getAllKeys(remoteRef, null);
        Map<String, AbsObjectInfo> ret = new TreeMap<>();
        // add all backups to the list that have usable metadata-files
        for (String key : allKeys)
        {
            try
            {
                AbsMetafileNameInfo info = null;
                BackupMetaDataPojo metaFile = null;
                AbsObjectInfo metaInfo = null;
                if (remoteRef instanceof ObsRemote)
                {
                    info = new ObsMetafileNameInfo(key);
                    // throws parse exception if not linstor json
                    metaFile = obsBackupHandler.getMetaFile(
                            key,
                            (ObsRemote) remoteRef,
                            peerAccCtx.get(),
                            getLocalMasterKey()
                    );
                    metaInfo = ret.computeIfAbsent(key, ObsObjectInfo::new);
                }
                else if (remoteRef instanceof S3Remote)
                {
                    info = new S3MetafileNameInfo(key);
                    // throws parse exception if not linstor json
                    metaFile = backupHandler.getMetaFile(
                            key,
                            (S3Remote) remoteRef,
                            peerAccCtx.get(),
                            getLocalMasterKey()
                    );
                    metaInfo = ret.computeIfAbsent(key, S3ObjectInfo::new);
                }

                metaInfo.exists = true;
                metaInfo.metaFile = metaFile;
                for (List<BackupMetaInfoPojo> backupInfoPojoList : metaFile.getBackups().values())
                {
                    for (BackupMetaInfoPojo backupInfoPojo : backupInfoPojoList)
                    {
                        if (remoteRef instanceof  ObsRemote)
                        {
                            String childKey = backupInfoPojo.getName();
                            ObsObjectInfo childObsObj = (ObsObjectInfo) ret.computeIfAbsent(childKey, ObsObjectInfo::new);
                            childObsObj.referencedBy.add(metaInfo);
                            metaInfo.references.add(childObsObj);
                        }
                        else if (remoteRef instanceof S3Remote)
                        {
                            String childKey = backupInfoPojo.getName();
                            S3ObjectInfo childS3Obj = (S3ObjectInfo) ret.computeIfAbsent(childKey, S3ObjectInfo::new);
                            childS3Obj.referencedBy.add(metaInfo);
                            metaInfo.references.add(childS3Obj);
                        }

                    }
                }

                SnapshotDefinition snapDfn = loadSnapDfnIfExists(info.rscName, info.snapName);
                if (snapDfn != null)
                {
                    if (snapDfn.getUuid().toString().equals(metaFile.getSnapDfnUuid()))
                    {
                        metaInfo.snapDfn = snapDfn;
                    }
                    else
                    {
                        apiCallRc.addEntry(
                            "Not marking SnapshotDefinition " + info.rscName + " / " + info.snapName +
                                " for exclusion as the UUID does not match with the backup",
                            ApiConsts.WARN_NOT_FOUND
                        );
                    }
                }

                String basedOnKey = metaFile.getBasedOn();
                if (basedOnKey != null)
                {
                    AbsObjectInfo basedOnMetaInfo = null;
                    if (remoteRef instanceof ObsRemote)
                    {
                        basedOnMetaInfo = ret.computeIfAbsent(basedOnKey, ObsObjectInfo::new);
                    }
                    else if (remoteRef instanceof S3Remote)
                    {
                        basedOnMetaInfo = ret.computeIfAbsent(basedOnKey, S3ObjectInfo::new);
                    }
                    basedOnMetaInfo.referencedBy.add(metaInfo);
                    metaInfo.references.add(basedOnMetaInfo);
                }
            }
            catch (MismatchedInputException exc)
            {
                // ignore, most likely an older format of linstor's backup .meta json
            }
            catch (IOException exc)
            {
                String errRepId = errorReporter.reportError(exc, peerAccCtx.get(), null, "used s3/obs key: " +key);
                apiCallRc.addEntry(
                    "IO exception while parsing metafile " + key + ". Details in error report " + errRepId,
                    ApiConsts.FAIL_UNKNOWN_ERROR
                );
            }
            catch (ParseException ignored)
            {
                // Ignored, not a meta file
            }

            try
            {
                AbsVolumeNameInfo info = null;
                AbsObjectInfo dataInfo = null;
                if (remoteRef instanceof ObsRemote)
                {
                    info = new ObsVolumeNameInfo(key);
                    dataInfo = ret.computeIfAbsent(key, ObsObjectInfo::new);
                }
                else if (remoteRef instanceof S3Remote)
                {
                    info = new S3VolumeNameInfo(key);
                    dataInfo = ret.computeIfAbsent(key, S3ObjectInfo::new);
                }

                dataInfo.exists = true;
                dataInfo.snapDfn = loadSnapDfnIfExists(info.rscName, info.snapName);
            }
            catch (ParseException ignored)
            {
                // Ignored, not a volume file
            }
        }

        return ret;
    }

    /**
     * Unlike {@link CtrlApiDataLoader#loadSnapshotDfn(String, String, boolean)} this method does not expect rscDfn to
     * exist when trying to load snapDfn
     *
     * @param rscName
     * @param snapName
     *
     * @return
     */
    SnapshotDefinition loadSnapDfnIfExists(String rscName, String snapName)
    {
        SnapshotDefinition snapDfn = null;
        try
        {
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, false);
            if (rscDfn != null)
            {
                snapDfn = rscDfn.getSnapshotDfn(
                    peerAccCtx.get(),
                    new SnapshotName(snapName)
                );
            }
        }
        catch (InvalidNameException | AccessDeniedException ignored)
        {
        }
        return snapDfn;
    }

    /**
     * Removes the snap from the "shipping started" list on the stlt. This action can't be done
     * by the stlt itself since that might be too early and therefore trigger a second shipping by an
     * unrelated update
     */
    public Flux<ApiCallRc> startStltCleanup(Peer peer, String rscNameRef, String snapNameRef)
    {
        byte[] msg = ctrlStltSerializer.headerlessBuilder().notifyBackupShippingFinished(rscNameRef, snapNameRef)
            .build();
        return peer.apiCall(InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_FINISHED, msg).map(
            inputStream -> CtrlSatelliteUpdateCaller.deserializeApiCallRc(
                peer.getNode().getName(),
                inputStream
            )
        );
    }

    public Flux<ApiCallRc> cleanupStltRemote(StltRemote remote)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Cleanup Stlt-Remote",
                lockGuardFactory.create()
                    .write(LockObj.REMOTE_MAP).buildDeferred(),
                () -> cleanupStltRemoteInTransaction(remote)
            );
    }

    private Flux<ApiCallRc> cleanupStltRemoteInTransaction(StltRemote remote)
    {
        Flux<ApiCallRc> flux;
        try
        {
            remote.getFlags().enableFlags(sysCtx, StltRemote.Flags.DELETE);
            ctrlTransactionHelper.commit();
            flux = ctrlSatelliteUpdateCaller.updateSatellites(remote)
                .concatWith(
                    scopeRunner.fluxInTransactionalScope(
                        "Removing temporary satellite remote",
                        lockGuardFactory.create()
                            .write(LockObj.REMOTE_MAP).buildDeferred(),
                        () -> deleteStltRemoteInTransaction(remote.getName().displayValue)
                    )
                );
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        return flux;
    }

    private Flux<ApiCallRc> deleteStltRemoteInTransaction(String remoteNameRef)
    {
        AbsRemote remote;
        try
        {
            remote = remoteRepo.get(sysCtx, new RemoteName(remoteNameRef, true));
            if (!(remote instanceof StltRemote))
            {
                throw new ImplementationError("This method should only be called for satellite remotes");
            }
            remoteRepo.remove(sysCtx, remote.getName());
            remote.delete(sysCtx);

            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException | InvalidNameException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        return Flux.empty();
    }

    AbsMetafileNameInfo getLatestBackup(Set<String> keys, String snapName, RemoteType remoteType)
    {
        AbsMetafileNameInfo latest = null;
        for (String key : keys)
        {
            try
            {
                AbsMetafileNameInfo current = null;
                if (remoteType.equals(RemoteType.OBS)) {
                    current = new ObsMetafileNameInfo(key);
                }
                else if (remoteType.equals(RemoteType.S3))
                {
                    current = new S3MetafileNameInfo(key);
                }

                if (snapName != null && !snapName.isEmpty() && !snapName.equals(current.snapName))
                {
                    // Snapshot names do not match, ignore this backup
                    continue;
                }
                if (latest == null || latest.backupTime.before(current.backupTime))
                {
                    latest = current;
                }
            }
            catch (ParseException e)
            {
                // Not a metadata file, ignore
            }
        }
        return latest;
    }

    static abstract class AbsObjectInfo
    {
        String key;
        boolean exists = false;
        BackupMetaDataPojo metaFile;
        SnapshotDefinition snapDfn = null;
        HashSet<AbsObjectInfo> referencedBy = new HashSet<>();
        HashSet<AbsObjectInfo> references = new HashSet<>();

        public AbsObjectInfo(String keyRef)
        {
            key = keyRef;
        }

        public boolean isMetaFile()
        {
            return metaFile != null;
        }

        public SnapshotDefinition getSnapDfn()
        {
            return snapDfn;
        }

        public boolean doesExist()
        {
            return exists;
        }

        public BackupMetaDataPojo getMetaFile()
        {
            return metaFile;
        }

        public String getKey()
        {
            return key;
        }

        public HashSet<AbsObjectInfo> getReferencedBy()
        {
            return referencedBy;
        }

        public HashSet<AbsObjectInfo> getReferences()
        {
            return references;
        }
    }

    static class S3ObjectInfo
        extends AbsObjectInfo
    {
        public S3ObjectInfo(String s3KeyRef)
        {
            super(s3KeyRef);
        }

        @Override
        public String toString()
        {
            return "S3ObjectInfo [s3Key=" + super.key + "]";
        }
    }

    static class ObsObjectInfo
        extends AbsObjectInfo
    {
        public ObsObjectInfo(String obsKeyRef)
        {
            super(obsKeyRef);
        }

        @Override
        public String toString()
        {
            return "ObsObjectInfo [obsKey=" + super.key + "]";
        }
    }
}
