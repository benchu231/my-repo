package com.linbit.linstor.backupshipping;

import java.text.ParseException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author mou
 * @create 2024-01-15 18:21
 */
public abstract class AbsMetafileNameInfo {

    public final String rscName;
    public final String backupId;
    public final Date backupTime;
    public final String suffix;
    public final String snapName;

    AbsMetafileNameInfo(String rscNameRef, Date backupTimeRef, String suffixRef, String snapNameRef)
    {
        rscName = rscNameRef;
        backupTime = backupTimeRef;
        backupId = BackupConsts.BACKUP_PREFIX + BackupConsts.format(backupTimeRef);
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

    public AbsMetafileNameInfo(String raw, Pattern pattern) throws ParseException
    {
        Matcher matcher = pattern.matcher(raw);
        if (!matcher.matches())
        {
            throw new ParseException("Failed to parse " + raw + " as s3/obs backup meta file", 0);
        }

        rscName = matcher.group("rscName");
        backupId = matcher.group("backupId");
        backupTime = BackupConsts.parse(backupId.substring(BackupConsts.BACKUP_PREFIX_LEN));
        suffix = BackupShippingUtils.defaultEmpty(matcher.group(this instanceof ObsMetafileNameInfo ? "obsSuffix" : "s3Suffix"));

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

    public String toFullBackupId()
    {
        String result = rscName + "_" + backupId + suffix;
        if (!snapName.isEmpty() && !backupId.equals(snapName))
        {
            result += BackupConsts.SNAP_NAME_SEPARATOR + snapName;
        }
        return result;
    }

    @Override
    public String toString()
    {
        return toFullBackupId() + ".meta";
    }
}
