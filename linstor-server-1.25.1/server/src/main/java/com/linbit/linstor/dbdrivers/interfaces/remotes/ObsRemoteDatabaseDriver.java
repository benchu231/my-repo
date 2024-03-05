package com.linbit.linstor.dbdrivers.interfaces.remotes;

import com.linbit.linstor.core.objects.remotes.ObsRemote;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

public interface ObsRemoteDatabaseDriver
{
    /**
     * Creates or updates the given ObsRemote object into the database.
     *
     * @param remote
     *
     * @throws DatabaseException
     */
    void create(ObsRemote remote) throws DatabaseException;

    /**
     * Removes the given ObsRemote object from the database
     *
     * @param remote
     *
     * @throws DatabaseException
     */
    void delete(ObsRemote remote) throws DatabaseException;

    SingleColumnDatabaseDriver<ObsRemote, String> getEndpointDriver();

    SingleColumnDatabaseDriver<ObsRemote, String> getBucketDriver();

    SingleColumnDatabaseDriver<ObsRemote, String> getRegionDriver();

    SingleColumnDatabaseDriver<ObsRemote, byte[]> getAccessKeyDriver();

    SingleColumnDatabaseDriver<ObsRemote, byte[]> getSecretKeyDriver();

    StateFlagsPersistence<ObsRemote> getStateFlagsPersistence();
}
