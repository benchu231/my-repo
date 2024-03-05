package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@Migration(
        version = "2023.12.25.12.00",
        description = "Add OBS_Remotes table"
)
public class Migration_2023_12_25_AddObsRemote extends LinstorMigration
{

    @Override
    protected void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        SQLUtils.runSql(
                connection,
                MigrationUtils.replaceTypesByDialect(dbProduct, MigrationUtils.loadResource("2023_12_25_add-obs-remotes.sql"))
        );
    }

}
