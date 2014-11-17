package solar.mr.io;


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
      builder.append(from.container().name()).append("!").append(from.path())
          .append("?available=").append(from.isAvailable())
          .append("&sorted=").append(from.isSorted());
      if (from.isAvailable()) {
        builder.append("&crc=").append(from.crc());
        builder.append("&length=").append(from.length());
        builder.append("&records=").append(from.recordsCount());
        builder.append("&keys=").append(from.keysCount());
      }
      builder.append("#").append(Long.toString(from.metaTS()));

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
      CharSequence[] parts = CharSeqTools.split(from, "!");
      final String env = parts[0].toString();
      if (!env.equals(wb.env().name()))
        throw new IllegalStateException("Serialized shard does not correspond to current environment");
      parts = CharSeqTools.split(parts[1], "?");
      final String path = parts[0].toString();
      parts = CharSeqTools.split(parts[1], "#");
      final long ts = CharSeqTools.parseLong(parts[1]);
      parts = CharSeqTools.split(parts[0], "&");
      long length = 0;
      long recordsCount = 0;
      long keysCount = 0;
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
      }

      return new MRTableShard(path, wb.env(), available, sorted, crc, length, keysCount, recordsCount, ts);
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
