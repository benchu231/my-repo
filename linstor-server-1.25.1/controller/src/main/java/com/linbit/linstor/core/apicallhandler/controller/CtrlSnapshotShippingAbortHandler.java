package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.*;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.BackupInfoManager.AbortInfo;
import com.linbit.linstor.core.BackupInfoManager.AbortBackupInfo;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.ObsRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.rmi.Remote;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlSnapshotShippingAbortHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final Provider<CtrlSnapshotDeleteApiCallHandler> snapDelHandlerProvider;
    private final BackupInfoManager backupInfoMgr;
    private final BackupToS3 backupHandler;
    private final BackupToObs obsBackupHandler;
    private final RemoteRepository remoteRepo;
    private final ErrorReporter errorReporter;
    private final CtrlSecurityObjects ctrlSecObj;

    @Inject
    public CtrlSnapshotShippingAbortHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockguardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        Provider<CtrlSnapshotDeleteApiCallHandler> snapDelHandlerProviderRef,
        BackupInfoManager backupInfoMgrRef,
        BackupToS3 backupHandlerRef,
        BackupToObs obsBackupHandlerRef,
        RemoteRepository remoteRepoRef,
        ErrorReporter errorReporterRef,
        CtrlSecurityObjects ctrlSecObjRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockguardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        snapDelHandlerProvider = snapDelHandlerProviderRef;
        backupInfoMgr = backupInfoMgrRef;
        backupHandler = backupHandlerRef;
        obsBackupHandler = obsBackupHandlerRef;
        remoteRepo = remoteRepoRef;
        errorReporter = errorReporterRef;
        ctrlSecObj = ctrlSecObjRef;
    }

    public Flux<ApiCallRc> abortAllShippingPrivileged(Node nodeRef, boolean abortMultiPartRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Abort all snapshot shipments to node",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> abortAllShippingPrivilegedInTransaction(nodeRef, abortMultiPartRef)
            );
    }

    private Flux<ApiCallRc> abortAllShippingPrivilegedInTransaction(Node nodeRef, boolean abortMultiPartRef)
    {
        Flux<ApiCallRc> flux = Flux.empty();
        try
        {
            for (Snapshot snap : nodeRef.getSnapshots(apiCtx))
            {
                SnapshotDefinition snapDfn = snap.getSnapshotDefinition();
                if (
                    snapDfn.getFlags().isSet(apiCtx, SnapshotDefinition.Flags.SHIPPING) &&
                        !snap.getFlags().isSet(apiCtx, Snapshot.Flags.BACKUP_TARGET) &&
                        snapDfn.getFlags().isUnset(apiCtx, SnapshotDefinition.Flags.BACKUP)
                )
                {
                    flux = flux.concatWith(abortBackupShippings(snapDfn, abortMultiPartRef));
                }
            }
            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return flux;
    }

    public Flux<ApiCallRc> abortSnapshotShippingPrivileged(ResourceDefinition rscDfn)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Abort snapshot shipments of rscDfn",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> abortSnapshotShippingPrivilegedInTransaction(rscDfn)
            );
    }

    private Flux<ApiCallRc> abortSnapshotShippingPrivilegedInTransaction(ResourceDefinition rscDfn)
    {
        Flux<ApiCallRc> flux = Flux.empty();
        try
        {

            for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(apiCtx))
            {
                if (
                    snapDfn.getFlags().isSet(apiCtx, SnapshotDefinition.Flags.SHIPPING) &&
                        !snapDfn.getFlags().isSet(apiCtx, SnapshotDefinition.Flags.BACKUP)
                )
                {
                    flux = flux.concatWith(
                        snapDelHandlerProvider.get()
                            .deleteSnapshot(
                                snapDfn.getResourceName().displayValue,
                                snapDfn.getName().displayValue,
                                null
                            )
                    );
                }
            }
            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return flux;
    }

    public Flux<ApiCallRc> abortBackupShippingPrivileged(SnapshotDefinition snapDfn, boolean abortMultiPartRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Abort backup shipments of rscDfn",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> abortBackupShippingPrivilegedInTransaction(snapDfn, abortMultiPartRef)
            );
    }

    private Flux<ApiCallRc> abortBackupShippingPrivilegedInTransaction(
        SnapshotDefinition snapDfn,
        boolean abortMultiPartRef
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        try
        {
            if (snapDfn.getFlags().isSet(apiCtx, SnapshotDefinition.Flags.SHIPPING, SnapshotDefinition.Flags.BACKUP))
            {
                for (Snapshot snap : snapDfn.getAllSnapshots(apiCtx))
                {
                    if (!snap.getFlags().isSet(apiCtx, Snapshot.Flags.BACKUP_TARGET))
                    {
                        flux = flux.concatWith(abortBackupShippings(snapDfn, abortMultiPartRef));
                        break;
                    }
                }
            }
            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return flux;
    }

    public void markSnapshotShippingAborted(SnapshotDefinition snapDfnRef)
    {
        try
        {
            snapDfnRef.getFlags().disableFlags(
                apiCtx,
                SnapshotDefinition.Flags.SHIPPING,
                SnapshotDefinition.Flags.SHIPPING_CLEANUP,
                SnapshotDefinition.Flags.SHIPPED
            );
            snapDfnRef.getFlags().enableFlags(apiCtx, SnapshotDefinition.Flags.SHIPPING_ABORT);
            ResourceName rscName = snapDfnRef.getResourceName();

            Collection<Snapshot> snapshots = snapDfnRef.getAllSnapshots(apiCtx);
            for (Snapshot snap : snapshots)
            {
                Resource rsc1 = snap.getNode().getResource(apiCtx, rscName);
                for (Snapshot snap2 : snapshots)
                {
                    if (snap != snap2)
                    {
                        Resource rsc2 = snap2.getNode().getResource(apiCtx, rscName);

                        ResourceConnection rscConn = rsc1.getAbsResourceConnection(apiCtx, rsc2);
                        if (rscConn != null)
                        {
                            rscConn.setSnapshotShippingNameInProgress(null);
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private Flux<ApiCallRc> abortBackupShippings(SnapshotDefinition snapDfn, boolean abortMultiPartRef)
    {
        Flux<ApiCallRc> flux = Flux.empty();
        boolean shouldAbort = false;
        try
        {
            for (Snapshot snap : snapDfn.getAllSnapshots(apiCtx))
            {
                Map<SnapshotDefinition.Key, AbortInfo> abortEntries = backupInfoMgr.abortCreateGetEntries(
                    snap.getNodeName()
                );
                if (abortEntries != null && !abortEntries.isEmpty())
                {
                    AbortInfo abortInfo = abortEntries.get(snapDfn.getSnapDfnKey());
                    if (abortInfo != null && !abortInfo.isEmpty())
                    {
                        shouldAbort = true;
                        if (abortMultiPartRef)
                        {
                            List<AbortBackupInfo> abortBackupList = new ArrayList<>();
                            abortBackupList.addAll(abortInfo.abortS3InfoList);
                            abortBackupList.addAll(abortInfo.abortObsInfoList);
                            for (AbortBackupInfo abortBackupInfo : abortBackupList)
                            {
                                try
                                {
                                    AbsRemote remote = remoteRepo.get(apiCtx, new RemoteName(abortBackupInfo.remoteName));
                                    byte[] masterKey = ctrlSecObj.getCryptKey();
                                    if (masterKey == null || masterKey.length == 0)
                                    {
                                        throw new ApiRcException(
                                            ApiCallRcImpl
                                                .entryBuilder(
                                                    ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY,
                                                    "Unable to decrypt the S3/OBS access key and secret key " +
                                                        "without having a master key"
                                                )
                                                .setCause("The masterkey was not initialized yet")
                                                .setCorrection("Create or enter the master passphrase")
                                                .build()
                                        );
                                    }
                                    if (remote instanceof ObsRemote)
                                    {
                                        obsBackupHandler.abortMultipart(
                                            abortBackupInfo.backupName,
                                            abortBackupInfo.uploadId,
                                            (ObsRemote) remote,
                                            apiCtx,
                                            masterKey
                                        );
                                    }
                                    else if (remote instanceof S3Remote) {
                                        backupHandler.abortMultipart(
                                            abortBackupInfo.backupName,
                                            abortBackupInfo.uploadId,
                                            (S3Remote) remote,
                                            apiCtx,
                                            masterKey
                                        );
                                    }

                                }
                                catch (SdkClientException exc)
                                {
                                    if (exc.getClass() == AmazonS3Exception.class)
                                    {
                                        AmazonS3Exception s3Exc = (AmazonS3Exception) exc;
                                        if (s3Exc.getStatusCode() != 404)
                                        {
                                            errorReporter.reportError(exc);
                                        }
                                    }
                                    else
                                    {
                                        errorReporter.reportError(exc);
                                    }
                                }
                                catch (InvalidNameException exc)
                                {
                                    throw new ImplementationError(exc);
                                }
                            }
                            // nothing to do for AbortL2LInfo entries, just enable the ABORT flag
                        }
                        backupInfoMgr.abortCreateDeleteEntries(snap.getNodeName(), snapDfn.getSnapDfnKey());
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        if (shouldAbort)
        {
            enableFlagsPrivileged(
                snapDfn,
                SnapshotDefinition.Flags.SHIPPING_ABORT
            );
            flux = snapDelHandlerProvider.get()
                .deleteSnapshot(
                    snapDfn.getResourceName().displayValue,
                    snapDfn.getName().displayValue,
                    null
                );
        }
        return flux;
    }

    private void enableFlagsPrivileged(SnapshotDefinition snapDfn, SnapshotDefinition.Flags... snapDfnFlags)
    {
        try
        {
            snapDfn.getFlags().enableFlags(apiCtx, snapDfnFlags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

}
