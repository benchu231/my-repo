package com.linbit.linstor.core.objects.remotes;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.api.pojo.ObsRemotePojo;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.remotes.ObsRemoteDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nonnull;
import javax.inject.Provider;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public class ObsRemote extends AbsRemote
{
    public interface InitMaps
    {
        // currently only a place holder for future maps
    }

    private final ObjectProtection objProt;
    private final ObsRemoteDatabaseDriver driver;
    private final TransactionSimpleObject<ObsRemote, String> endpoint;
    private final TransactionSimpleObject<ObsRemote, String> bucket;
    private final TransactionSimpleObject<ObsRemote, String> region;
    private final TransactionSimpleObject<ObsRemote, byte[]> accessKey;
    private final TransactionSimpleObject<ObsRemote, byte[]> secretKey;
    private final TransactionSimpleObject<ObsRemote, Boolean> deleted;
    private final StateFlags<Flags> flags;

    // it would be nicer if both booleans could be TransactionSimpleObjects
    // but the stlt changes these as well, and would require scopes in the corresponding threads only for this
    // which is rather cumbersome
    private boolean requesterPaysSupported = true;
    private boolean multiDeleteSupported = true;

    public ObsRemote(
            ObjectProtection objProtRef,
            UUID objIdRef,
            ObsRemoteDatabaseDriver driverRef,
            RemoteName remoteNameRef,
            long initialFlags,
            String endpointRef,
            String bucketRef,
            String regionRef,
            byte[] accessKeyRef,
            byte[] secretKeyRef,
            TransactionObjectFactory transObjFactory,
            Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(objIdRef, transObjFactory, transMgrProvider, objProtRef, remoteNameRef);
        objProt = objProtRef;
        driver = driverRef;

        endpoint = transObjFactory.createTransactionSimpleObject(this, endpointRef, driver.getEndpointDriver());
        bucket = transObjFactory.createTransactionSimpleObject(this, bucketRef, driver.getBucketDriver());
        region = transObjFactory.createTransactionSimpleObject(this, regionRef, driver.getRegionDriver());
        accessKey = transObjFactory.createTransactionSimpleObject(this, accessKeyRef, driver.getAccessKeyDriver());
        secretKey = transObjFactory.createTransactionSimpleObject(this, secretKeyRef, driver.getSecretKeyDriver());

        flags = transObjFactory
                .createStateFlagsImpl(objProt, this, Flags.class, driver.getStateFlagsPersistence(), initialFlags);

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
                objProt,
                endpoint,
                bucket,
                region,
                accessKey,
                secretKey,
                flags,
                deleted
        );
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public int compareTo(@Nonnull AbsRemote remote)
    {
        int cmp = remote.getClass().getSimpleName().compareTo(ObsRemote.class.getSimpleName());
        if (cmp == 0)
        {
            cmp = remoteName.compareTo(remote.getName());
        }
        return cmp;
    }


    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(remoteName);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof ObsRemote)
        {
            ObsRemote other = (ObsRemote) obj;
            other.checkDeleted();
            ret = Objects.equals(remoteName, other.remoteName);
        }
        return ret;
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public RemoteName getName()
    {
        checkDeleted();
        return remoteName;
    }

    public String getUrl(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return endpoint.get();
    }

    public void setUrl(AccessContext accCtx, String url) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        endpoint.set(url);
    }

    public String getBucket(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return bucket.get();
    }

    public void setBucket(AccessContext accCtx, String bucketRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        bucket.set(bucketRef);
    }

    public String getRegion(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return region.get();
    }

    public void setRegion(AccessContext accCtx, String regionRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        region.set(regionRef);
    }

    public byte[] getAccessKey(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return accessKey.get();
    }

    public void setAccessKey(AccessContext accCtx, byte[] accessKeyRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        accessKey.set(accessKeyRef);
    }

    public byte[] getSecretKey(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return secretKey.get();
    }

    public void setSecretKey(AccessContext accCtx, byte[] secretKeyRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        secretKey.set(secretKeyRef);
    }

    public boolean isRequesterPaysSupported(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return requesterPaysSupported;
    }

    public void setRequesterPaysSupported(AccessContext accCtx, boolean requesterPaysSupportedRef)
            throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        requesterPaysSupported = requesterPaysSupportedRef;
    }

    public boolean isMultiDeleteSupported(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return multiDeleteSupported;
    }

    public void setMultiDeleteSupported(AccessContext accCtx, boolean multiDeleteSupportedRef)
            throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        multiDeleteSupported = multiDeleteSupportedRef;
    }

    @Override
    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public RemoteType getType()
    {
        return RemoteType.OBS;
    }

    public ObsRemotePojo getApiData(AccessContext accCtx, Long fullSyncId, Long updateId) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return new ObsRemotePojo(
                objId,
                remoteName.displayValue,
                flags.getFlagsBits(accCtx),
                endpoint.get(),
                bucket.get(),
                region.get(),
                accessKey.get(),
                secretKey.get(),
                fullSyncId,
                updateId
        );
    }

    public void applyApiData(AccessContext accCtx, ObsRemotePojo apiData) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        endpoint.set(apiData.getEndpoint());
        bucket.set(apiData.getBucket());
        region.set(apiData.getRegion());
        accessKey.set(apiData.getAccessKey());
        secretKey.set(apiData.getSecretKey());

        flags.resetFlagsTo(accCtx, Flags.restoreFlags(apiData.getFlags()));
    }

    @Override
    public boolean isDeleted()
    {
        return deleted.get();
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            objProt.delete(accCtx);

            activateTransMgr();

            driver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    protected void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted ObsRemote");
        }
    }

    @Override
    protected String toStringImpl()
    {
        return "ObsRemote '" + remoteName.displayValue + "'";
    }
}
