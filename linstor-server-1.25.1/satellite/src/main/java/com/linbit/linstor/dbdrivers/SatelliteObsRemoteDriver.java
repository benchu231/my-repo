package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.remotes.ObsRemote;
import com.linbit.linstor.dbdrivers.interfaces.remotes.ObsRemoteDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteObsRemoteDriver
        extends AbsSatelliteDbDriver<ObsRemote>
        implements ObsRemoteDatabaseDriver
{
    private final StateFlagsPersistence<ObsRemote> stateFlagsDriver;
    private final SingleColumnDatabaseDriver<ObsRemote, String> endpointDriver;
    private final SingleColumnDatabaseDriver<ObsRemote, String> bucketDriver;
    private final SingleColumnDatabaseDriver<ObsRemote, String> regionDriver;
    private final SingleColumnDatabaseDriver<ObsRemote, byte[]> accessKeyDriver;
    private final SingleColumnDatabaseDriver<ObsRemote, byte[]> secretKeyDriver;

    @Inject
    public SatelliteObsRemoteDriver()
    {
        stateFlagsDriver = getNoopFlagDriver();
        endpointDriver = getNoopColumnDriver();
        bucketDriver = getNoopColumnDriver();
        regionDriver = getNoopColumnDriver();
        accessKeyDriver = getNoopColumnDriver();
        secretKeyDriver = getNoopColumnDriver();
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
        return stateFlagsDriver;
    }
}