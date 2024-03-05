package com.linbit.linstor.backupshipping;

import java.text.ParseException;
import java.util.Date;
import java.util.regex.Pattern;

public class S3MetafileNameInfo
    extends AbsMetafileNameInfo
{
    private static final Pattern META_FILE_PATTERN = Pattern.compile(
        "^(?<rscName>[a-zA-Z0-9_-]{2,48})_(?<backupId>back_[0-9]{8}_[0-9]{6})" +
            // in case of "<backupid>^<snapName>" we want to prioritize this combination instead of matching
            // "^<snapName>" as s3Suffix and leaving the snapName empty.

            // please note that "(?:|...)" behaves differently than "(...)?" since the former prioritizes the empty
            // group whereas the latter prioritizes a filled group
            "(?:|(?<s3Suffix>:?.+?))(?<snapName>\\^.*)?\\.meta$"
    );

    public S3MetafileNameInfo(String rscNameRef, Date backupTimeRef, String s3SuffixRef, String snapNameRef)
    {
        super(rscNameRef, backupTimeRef, s3SuffixRef, snapNameRef);
    }

    public S3MetafileNameInfo(String raw) throws ParseException
    {
        super(raw, META_FILE_PATTERN);
    }
}
