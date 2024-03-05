package com.linbit.linstor.backupshipping;

import java.text.ParseException;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * @author mou
 * @create 2024-01-06 18:30
 */
public class ObsVolumeNameInfo
    extends AbsVolumeNameInfo
{
    private static final Pattern BACKUP_VOLUME_PATTERN = Pattern.compile(
            "^(?<rscName>[a-zA-Z0-9_-]{2,48})(?<rscSuffix>\\..+)?_(?<vlmNr>[0-9]{5})_(?<backupId>back_[0-9]{8}_[0-9]{6})" +
                    "(?<obsSuffix>:?.+?)?(?<snapName>\\^.*)?$"
    );

    public ObsVolumeNameInfo(
            String rscNameRef,
            String layerSuffixRef,
            int vlmNrRef,
            Date backupTimeRef,
            String obsSuffixRef,
            String snapNameRef
    )
    {
        super(rscNameRef, layerSuffixRef, vlmNrRef, backupTimeRef, obsSuffixRef, snapNameRef);
    }

    public ObsVolumeNameInfo(String raw) throws ParseException
    {
        super(raw, BACKUP_VOLUME_PATTERN);
    }

    @Override
    public String toString()
    {
        String result = String.format("%s%s_%05d_%s%s", rscName, layerSuffix, vlmNr, backupId, suffix);
        if (!snapName.isEmpty() && !backupId.equals(snapName))
        {
            result += BackupConsts.SNAP_NAME_SEPARATOR + snapName;
        }
        return result;
    }

}

