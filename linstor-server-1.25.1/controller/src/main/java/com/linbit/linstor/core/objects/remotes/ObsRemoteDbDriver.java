package com.linbit.linstor.core.objects.remotes;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.ObsRemote.InitMaps;
import com.linbit.linstor.dbdrivers.*;
import com.linbit.linstor.dbdrivers.interfaces.remotes.ObsRemoteCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Base64;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.function.Function;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ObsRemotes.*;

@Singleton
public class ObsRemoteDbDriver extends AbsDatabaseDriver<ObsRemote, InitMaps, Void>
        implements ObsRemoteCtrlDatabaseDriver
{
    protected final PropsContainerFactory propsContainerFactory;
    protected final TransactionObjectFactory transObjFactory;
    protected final Provider<? extends TransactionMgr> transMgrProvider;

    protected final SingleColumnDatabaseDriver<ObsRemote, String> endpointDriver;
    protected final SingleColumnDatabaseDriver<ObsRemote, String> bucketDriver;
    protected final SingleColumnDatabaseDriver<ObsRemote, String> regionDriver;
    protected final SingleColumnDatabaseDriver<ObsRemote, byte[]> accessKeyDriver;
    protected final SingleColumnDatabaseDriver<ObsRemote, byte[]> secretKeyDriver;
    protected final StateFlagsPersistence<ObsRemote> flagsDriver;
    protected final AccessContext dbCtx;

    @Inject
    public ObsRemoteDbDriver(
            ErrorReporter errorReporterRef,
            @SystemContext AccessContext dbCtxRef,
            DbEngine dbEngine,
            Provider<TransactionMgr> transMgrProviderRef,
            ObjectProtectionFactory objProtFactoryRef,
            PropsContainerFactory propsContainerFactoryRef,
            TransactionObjectFactory transObjFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.OBS_REMOTES, dbEngine, objProtFactoryRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, remote -> remote.getUuid().toString());
        setColumnSetter(NAME, remote -> remote.getName().value);
        setColumnSetter(DSP_NAME, remote -> remote.getName().displayValue);
        setColumnSetter(FLAGS, remote -> remote.getFlags().getFlagsBits(dbCtx));
        setColumnSetter(ENDPOINT, remote -> remote.getUrl(dbCtx));
        setColumnSetter(BUCKET, remote -> remote.getBucket(dbCtx));
        setColumnSetter(REGION, remote -> remote.getRegion(dbCtx));
        switch (getDbType())
        {
            case ETCD:
                setColumnSetter(ACCESS_KEY, remote -> Base64.encode(remote.getAccessKey(dbCtx)));
                setColumnSetter(SECRET_KEY, remote -> Base64.encode(remote.getSecretKey(dbCtx)));
                break;
            case SQL: // fall-through
            case K8S_CRD:
                setColumnSetter(ACCESS_KEY, remote -> remote.getAccessKey(dbCtx));
                setColumnSetter(SECRET_KEY, remote -> remote.getSecretKey(dbCtx));
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        endpointDriver = generateSingleColumnDriver(ENDPOINT, remote -> remote.getUrl(dbCtx), Function.identity());
        bucketDriver = generateSingleColumnDriver(BUCKET, remote -> remote.getBucket(dbCtx), Function.identity());
        regionDriver = generateSingleColumnDriver(REGION, remote -> remote.getRegion(dbCtx), Function.identity());
        switch (getDbType())
        {
            case ETCD:
                accessKeyDriver = generateSingleColumnDriver(
                        ACCESS_KEY,
                        ignored -> MSG_DO_NOT_LOG,
                        byteArr -> Base64.encode(byteArr)
                );
                secretKeyDriver = generateSingleColumnDriver(
                        SECRET_KEY,
                        ignored -> MSG_DO_NOT_LOG,
                        byteArr -> Base64.encode(byteArr)
                );
                break;
            case SQL: // fall-through
            case K8S_CRD:
                accessKeyDriver = generateSingleColumnDriver(
                        ACCESS_KEY, ignored -> MSG_DO_NOT_LOG, Function.identity()
                );
                secretKeyDriver = generateSingleColumnDriver(
                        SECRET_KEY, ignored -> MSG_DO_NOT_LOG, Function.identity()
                );
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        flagsDriver = generateFlagDriver(FLAGS, ObsRemote.Flags.class);

    }

    @Override
    public SingleColumnDatabaseDriver<ObsRemote, String> getEndpointDriver()
    {
        return endpointDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ObsRemote, String> getBucketDriver()
    {
        return bucketDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ObsRemote, String> getRegionDriver()
    {
        return regionDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ObsRemote, byte[]> getAccessKeyDriver()
    {
        return accessKeyDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ObsRemote, byte[]> getSecretKeyDriver()
    {
        return secretKeyDriver;
    }

    @Override
    public StateFlagsPersistence<ObsRemote> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    protected Pair<ObsRemote, InitMaps> load(RawParameters raw, Void ignored)
            throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final RemoteName remoteName = raw.<String, RemoteName, InvalidNameException>build(DSP_NAME, RemoteName::new);
        final long initFlags;
        final byte[] accessKey;
        final byte[] secretKey;
        switch (getDbType())
        {
            case ETCD:
                initFlags = Long.parseLong(raw.get(FLAGS));
                accessKey = Base64.decode(raw.get(ACCESS_KEY));
                secretKey = Base64.decode(raw.get(SECRET_KEY));
                break;
            case SQL: // fall-through
            case K8S_CRD:
                initFlags = raw.get(FLAGS);
                accessKey = raw.get(ACCESS_KEY);
                secretKey = raw.get(SECRET_KEY);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }
        return new Pair<>(
                new ObsRemote(
                        getObjectProtection(ObjectProtection.buildPath(remoteName)),
                        raw.build(UUID, java.util.UUID::fromString),
                        this,
                        remoteName,
                        initFlags,
                        raw.get(ENDPOINT),
                        raw.get(BUCKET),
                        raw.get(REGION),
                        accessKey,
                        secretKey,
                        transObjFactory,
                        transMgrProvider
                ),
                new InitMapsImpl()
        );
    }

    @Override
    protected String getId(ObsRemote dataRef) throws AccessDeniedException
    {
        return "ObsRemote(" + dataRef.getName().displayValue + ")";
    }

    private class InitMapsImpl implements InitMaps
    {
        private InitMapsImpl()
        {
        }
    }
}
