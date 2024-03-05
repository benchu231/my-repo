package com.linbit.linstor.backupshipping;

import java.text.ParseException;
import java.util.Date;
import java.util.regex.Pattern;

public class S3VolumeNameInfo
    extends AbsVolumeNameInfo
{
    private static final Pattern BACKUP_VOLUME_PATTERN = Pattern.compile(
        "^(?<rscName>[a-zA-Z0-9_-]{2,48})(?<rscSuffix>\\..+)?_(?<vlmNr>[0-9]{5})_(?<backupId>back_[0-9]{8}_[0-9]{6})" +
            "(?<s3Suffix>:?.+?)?(?<snapName>\\^.*)?$"
    );

    public S3VolumeNameInfo(
        String rscNameRef,
        String layerSuffixRef,
        int vlmNrRef,
        Date backupTimeRef,
        String s3SuffixRef,
        String snapNameRef
    )
    {
        super(rscNameRef, layerSuffixRef, vlmNrRef, backupTimeRef, s3SuffixRef, snapNameRef);
    }

    public S3VolumeNameInfo(String raw) throws ParseException
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
