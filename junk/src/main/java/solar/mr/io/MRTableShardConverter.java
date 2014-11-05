package solar.mr.io;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.types.ConversionPack;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.proc.MRWhiteboard;
import solar.mr.tables.MRTableShard;

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
      builder.append(from.path()).append("@").append(from.container().name()).append("&").append(Long.toString(from.metaTS())).append("/").append(from.isAvailable());
      if (from.isAvailable()) {
        builder.append("/").append(from.crc());
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
      CharSequence[] split = CharSeqTools.split(from, "@");
      final String path = split[0].toString();
      split = CharSeqTools.split(split[1], "&");
      final String env = split[0].toString();
      if (!env.equals(wb.env().name())) {
        throw new IllegalStateException("Serialized shard does not correspond to current environment");
      }
      split = CharSeqTools.split(split[1], "/");
      final long ts = CharSeqTools.parseLong(split[0]);
      final boolean available = CharSeqTools.parseBoolean(split[1]);
      final String crc = available ? split[2].toString() : "";
      return wb.env().restore(path, ts, available, crc);
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
