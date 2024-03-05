package com.linbit.linstor.dbdrivers.interfaces.remotes;

import com.linbit.linstor.core.objects.remotes.ObsRemote;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;


public interface ObsRemoteCtrlDatabaseDriver
        extends ObsRemoteDatabaseDriver, ControllerDatabaseDriver<ObsRemote, ObsRemote.InitMaps, Void>
{

}
