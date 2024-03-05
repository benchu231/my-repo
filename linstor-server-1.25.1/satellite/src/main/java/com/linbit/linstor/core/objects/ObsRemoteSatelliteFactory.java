package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.DivergentUuidsException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.ObsRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.remotes.ObsRemoteDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.remotes.S3RemoteDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.UUID;

public class ObsRemoteSatelliteFactory
{
    private final ObsRemoteDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final RemoteMap remoteMap;

    @Inject
    public ObsRemoteSatelliteFactory(
            CoreModule.RemoteMap remoteMapRef,
            ObsRemoteDatabaseDriver driverRef,
            ObjectProtectionFactory objectProtectionFactoryRef,
            TransactionObjectFactory transObjFactoryRef,
            Provider<TransactionMgr> transMgrProviderRef
    )
    {
        remoteMap = remoteMapRef;
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public ObsRemote getInstanceSatellite(
            AccessContext accCtx,
            UUID uuid,
            RemoteName remoteNameRef,
            long initflags,
            String endpointRef,
            String bucketRef,
            String regionRef,
            byte[] accessKeyRef,
            byte[] secretKeyRef
    )
            throws ImplementationError
    {
        AbsRemote remote = remoteMap.get(remoteNameRef);
        ObsRemote obsRemote = null;
        if (remote == null)
        {
            try
            {
                obsRemote = new ObsRemote(
                        objectProtectionFactory.getInstance(accCtx, "", true),
                        uuid,
                        driver,
                        remoteNameRef,
                        initflags,
                        endpointRef,
                        bucketRef,
                        regionRef,
                        accessKeyRef,
                        secretKeyRef,
                        transObjFactory,
                        transMgrProvider
                );
                remoteMap.put(remoteNameRef, obsRemote);
            }
            catch (DatabaseException exc)
            {
                throw new ImplementationError(exc);
            }
        }
        else
        {
            if (!remote.getUuid().equals(uuid))
            {
                throw new DivergentUuidsException(
                        ObsRemote.class.getSimpleName(),
                        remote.getName().displayValue,
                        remoteNameRef.displayValue,
                        remote.getUuid(),
                        uuid
                );
            }
            if (remote instanceof ObsRemote)
            {
                obsRemote = (ObsRemote) remote;
            }
            else
            {
                throw new ImplementationError(
                        "Unknown implementation of Remote detected: " + remote.getClass().getCanonicalName()
                );
            }
        }
        return obsRemote;
    }
}
