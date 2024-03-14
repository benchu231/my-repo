package com.linbit.linstor.backupshipping;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.BackupToObs;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.StltConnTracker;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.core.objects.remotes.ObsRemote;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.locks.LockGuardFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.text.ParseException;
import java.util.function.BiConsumer;

@Singleton
public class BackupShippingObsService extends AbsBackupShippingService
{
    public static final String SERVICE_INFO = "BackupShippingObsService";

    protected static final String CMD_FORMAT_SENDING =
            "set -o pipefail; " +
                    "trap 'kill -HUP 0' SIGTERM; " +
                    "(" +
                    "%s | " +  // thin_send prev_LV_snapshot cur_LV_snapshot
                    // "pv -s 100m -bnr -i 0.1 | " +
                    "zstd;" +
                    ")&\\wait $!";

    protected static final String CMD_FORMAT_RECEIVING = "trap 'kill -HUP 0' SIGTERM; " +
            "exec 7<&0 0</dev/null; " +
            "set -o pipefail; " +
            "(" +
            "exec 0<&7 7<&-; zstd -d | " +
            // "pv -s 100m -bnr -i 0.1 | " +
            "%s ;" +
            ") & wait $!";

    private final BackupToObs backupHandler;

    @Inject
    public BackupShippingObsService(
            BackupToObs backupHandlerRef,
            ErrorReporter errorReporterRef,
            ExtCmdFactory extCmdFactoryRef,
            ControllerPeerConnector controllerPeerConnectorRef,
            CtrlStltSerializer interComSerializerRef,
            @SystemContext AccessContext accCtxRef,
            StltSecurityObjects stltSecObjRef,
            StltConfigAccessor stltConfigAccessorRef,
            StltConnTracker stltConnTracker,
            RemoteMap remoteMapRef,
            LockGuardFactory lockGuardFactoryRef
    )
    {
        super(
                errorReporterRef,
                SERVICE_INFO,
                RemoteType.OBS,
                extCmdFactoryRef,
                controllerPeerConnectorRef,
                interComSerializerRef,
                accCtxRef,
                stltSecObjRef,
                stltConfigAccessorRef,
                stltConnTracker,
                remoteMapRef,
                lockGuardFactoryRef
        );

        backupHandler = backupHandlerRef;
    }

    @Override
    protected String getCommandReceiving(String cmdRef, AbsRemote ignoredRemote, AbsStorageVlmData<Snapshot> ignored)
    {
        return String.format(CMD_FORMAT_RECEIVING, cmdRef);
    }

    @Override
    protected String getCommandSending(String cmdRef, AbsRemote ignoredRemote, AbsStorageVlmData<Snapshot> ignored)
    {
        return String.format(CMD_FORMAT_SENDING, cmdRef);
    }

    @Override
    protected String getBackupNameForRestore(AbsStorageVlmData<Snapshot> snapVlmDataRef)
            throws InvalidKeyException, AccessDeniedException
    {
        Snapshot snap = snapVlmDataRef.getVolume().getAbsResource();
        String backupId = snap.getProps(accCtx).getProp(
                InternalApiConsts.KEY_BACKUP_TO_RESTORE,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
        );

        try
        {
            ObsMetafileNameInfo info = new ObsMetafileNameInfo(backupId);

            return new ObsVolumeNameInfo(
                    info.rscName,
                    snapVlmDataRef.getRscLayerObject().getResourceNameSuffix(),
                    snapVlmDataRef.getVlmNr().value,
                    info.backupTime,
                    info.suffix,
                    info.snapName
            ).toString();
        }
        catch (ParseException exc)
        {
            throw new ImplementationError(
                    "The simplified backup-name " + backupId + " does not conform to the expected format."
            );
        }
    }

    @Override
    protected BackupShippingDaemon createDaemon(
            AbsStorageVlmData<Snapshot> snapVlmDataRef,
            String[] fullCommand,
            String backupNameRef,
            AbsRemote remote,
            boolean restore,
            Integer ignored,
            BiConsumer<Boolean, Integer> postAction
    )
    {
        return new BackupShippingObsDaemon(
                errorReporter,
                threadGroup,
                "shipping_" + backupNameRef,
                fullCommand,
                backupNameRef,
                (ObsRemote) remote,
                backupHandler,
                restore,
                snapVlmDataRef.getAllocatedSize() == -1 && snapVlmDataRef.getSnapshotAllocatedSize() != null ?
                        snapVlmDataRef.getSnapshotAllocatedSize() :
                        snapVlmDataRef.getAllocatedSize(),
                postAction,
                accCtx,
                stltSecObj.getCryptKey()
        );
    }

    @Override
    protected boolean preCtrlNotifyBackupShipped(
            boolean success,
            boolean restoring,
            Snapshot snap,
            ShippingInfo shippingInfo
    )
    {
        boolean successRet = success;
        if (success && !restoring)
        {
            try
            {
                ObsRemote obsRemote = (ObsRemote) shippingInfo.remote;

                backupHandler.putObject(
                        shippingInfo.objMetaKey,
                        fillPojo(snap, shippingInfo.basedOnObjMetaKey, shippingInfo),
                        obsRemote,
                        accCtx,
                        stltSecObj.getCryptKey()
                );
            }
            catch (InvalidKeyException | AccessDeniedException | IOException | ParseException exc)
            {
                errorReporter.reportError(new ImplementationError(exc));
                successRet = false;
            }
        }
        return successRet;
    }

    @Override
    protected void postAllBackupPartsRegistered(Snapshot snapRef, ShippingInfo infoRef)
    {
        // ignored
    }
}
