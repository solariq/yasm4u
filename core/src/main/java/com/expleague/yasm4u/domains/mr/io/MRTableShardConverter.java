package com.expleague.yasm4u.domains.mr.io;


import com.expleague.commons.func.types.ConversionPack;
import com.expleague.commons.func.types.TypeConverter;
import com.expleague.commons.seq.CharSeqTools;
import com.expleague.yasm4u.domains.mr.ops.impl.MRTableState;

/**
 * User: solar
 * Date: 21.10.14
 * Time: 17:10
 */
public class MRTableShardConverter implements ConversionPack<MRTableState,CharSequence> {
  public static class To implements TypeConverter<MRTableState, CharSequence> {
    @Override
    public CharSequence convert(final MRTableState from) {
      final StringBuilder builder = new StringBuilder();
      builder.append(from.path())
          .append("?available=").append(from.isAvailable())
          .append("&sorted=").append(from.isSorted());
      if (from.isAvailable()) {
        builder.append("&crc=").append(from.crc());
        builder.append("&length=").append(from.length());
        builder.append("&records=").append(from.recordsCount());
        builder.append("&keys=").append(from.keysCount());
        builder.append("&modtime=").append(from.modtime());
      }
      builder.append("#").append(Long.toString(from.snapshotTime()));

      return builder.toString();
    }
  }

  public static class From implements TypeConverter<CharSequence, MRTableState> {
    @Override
    public MRTableState convert(final CharSequence from) {
      CharSequence[] parts = CharSeqTools.split(from, "?");
      final String path = parts[0].toString();
      parts = CharSeqTools.split(parts[1], "#");
      final long ts = CharSeqTools.parseLong(parts[1]);
      parts = CharSeqTools.split(parts[0], "&");
      long length = 0;
      long recordsCount = 0;
      long keysCount = 0;
      long modtime = 0;
      boolean available = false;
      boolean sorted = false;
      String crc = "0";
      for(int i = 0; i < parts.length; i++) {
        final CharSequence part = parts[i];
        final CharSequence[] kv = CharSeqTools.split(part, "=");
        if (kv[0].equals("available"))
          available = kv[1].equals("true");
        if (kv[0].equals("sorted"))
          sorted = kv[1].equals("true");
        if (kv[0].equals("crc"))
          crc = kv[1].toString();
        if (kv[0].equals("length"))
          length = CharSeqTools.parseLong(kv[1]);
        if (kv[0].equals("records"))
          recordsCount = CharSeqTools.parseLong(kv[1]);
        if (kv[0].equals("keys"))
          keysCount = CharSeqTools.parseLong(kv[1]);
        if (kv[0].equals("modtime"))
          modtime = CharSeqTools.parseLong(kv[1]);
      }

      return new MRTableState(path, available, sorted, crc, length, keysCount, recordsCount, modtime, ts);
    }
  }
  @Override
  public Class<? extends TypeConverter<MRTableState, CharSequence>> to() {
    return To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSequence, MRTableState>> from() {
    return From.class;
  }
}
