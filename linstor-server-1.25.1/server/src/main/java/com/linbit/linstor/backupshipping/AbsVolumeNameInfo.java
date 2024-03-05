package com.linbit.linstor.backupshipping;

import java.text.ParseException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author mou
 * @create 2024-01-15 19:09
 */
public abstract class AbsVolumeNameInfo {

    private static Pattern BACKUP_VOLUME_PATTERN;

    public final String rscName;
    public final String layerSuffix;
    public final int vlmNr;
    public final String backupId;
    public final Date backupTime;
    public final String suffix;
    public final String snapName;

    public AbsVolumeNameInfo(
        String rscNameRef,
        String layerSuffixRef,
        int vlmNrRef,
        Date backupTimeRef,
        String suffixRef,
        String snapNameRef
    )
    {
        rscName = rscNameRef;
        layerSuffix = BackupShippingUtils.defaultEmpty(layerSuffixRef);
        vlmNr = vlmNrRef;
        backupId = BackupConsts.BACKUP_PREFIX + BackupConsts.format(backupTimeRef);
        backupTime = backupTimeRef;
        suffix = BackupShippingUtils.defaultEmpty(suffixRef);
        if (snapNameRef == null || snapNameRef.isEmpty())
        {
            snapName = backupId;
        }
        else
        {
            snapName = snapNameRef;
        }
    }

    public AbsVolumeNameInfo(String raw, Pattern pattern) throws ParseException
    {
        Matcher matcher = pattern.matcher(raw);
        if (!matcher.matches())
        {
            throw new ParseException("Failed to parse " + raw + " as S3/OBS backup meta file", 0);
        }

        rscName = matcher.group("rscName");
        backupId = matcher.group("backupId");
        layerSuffix = BackupShippingUtils.defaultEmpty(matcher.group("rscSuffix"));
        vlmNr = Integer.parseInt(matcher.group("vlmNr"));
        backupTime = BackupConsts.parse(backupId.substring(BackupConsts.BACKUP_PREFIX_LEN));

        String name = "";
        if (this instanceof ObsVolumeNameInfo)
        {
            name = "obsSuffix";
        }
        else
        if (this instanceof S3VolumeNameInfo)
        {
            name = "s3Suffix";
        }
        suffix = BackupShippingUtils.defaultEmpty(matcher.group(name));

        String snapNameRef = BackupShippingUtils.defaultEmpty(matcher.group("snapName"));

        if (snapNameRef.startsWith(BackupConsts.SNAP_NAME_SEPARATOR))
        {
            snapNameRef = snapNameRef.substring(BackupConsts.SNAP_NAME_SEPARATOR_LEN);
        }

        if (snapNameRef.isEmpty())
        {
            snapNameRef = backupId;
        }

        snapName = snapNameRef;
    }

}
