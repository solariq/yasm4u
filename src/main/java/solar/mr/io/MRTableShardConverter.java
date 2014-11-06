package solar.mr.io;

import java.net.URI;


import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.types.ConversionPack;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.proc.MRWhiteboard;
import solar.mr.MRTableShard;

/**
 * User: solar
 * Date: 21.10.14
 * Time: 17:10
 */
public class MRTableShardConverter implements ConversionPack<MRTableShard,CharSequence> {
  public static class To implements TypeConverter<MRTableShard, CharSequence> {
    @Override
    public CharSequence convert(final MRTableShard from) {
      final StringBuilder builder = new StringBuilder();
      builder.append(from.container().name())
          .append(":").append(Long.toString(from.metaTS()))
          .append("/").append(from.path())
          .append("?available=").append(from.isAvailable())
          .append("&sorted=").append(from.isSorted());
      if (from.isAvailable()) {
        builder.append("&crc=").append(from.crc());
      }
      return builder.toString();
    }
  }

  public static class From implements TypeConverter<CharSequence, MRTableShard>, Action<MRWhiteboard> {
    private MRWhiteboard wb;

    @Override
    public void invoke(final MRWhiteboard wb) {
      this.wb = wb;
    }

    @Override
    public MRTableShard convert(final CharSequence from) {
      final int hostStart = CharSeqTools.indexOf(from, ":");
      final int tsStart = CharSeqTools.indexOf(from, hostStart + 1, ":");
      final int pathStart = CharSeqTools.indexOf(from, tsStart + 1, "/");

      final String env = from.subSequence(0, tsStart).toString();
      if (!env.equals(wb.env().name()))
        throw new IllegalStateException("Serialized shard does not correspond to current environment");
      final long ts = CharSeqTools.parseLong(from.subSequence(tsStart + 1, pathStart));
      CharSequence[] parts = CharSeqTools.split(from.subSequence(pathStart + 1, from.length()), "?");
      final String path = parts[0].toString();
      parts = CharSeqTools.split(parts[1], "&");
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
        if (kv[0].equals("available"))
          crc = kv[1].toString();
      }

      return new MRTableShard(path, wb.env(), available, sorted, crc);
    }
  }
  @Override
  public Class<? extends TypeConverter<MRTableShard, CharSequence>> to() {
    return To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSequence, MRTableShard>> from() {
    return From.class;
  }
}
