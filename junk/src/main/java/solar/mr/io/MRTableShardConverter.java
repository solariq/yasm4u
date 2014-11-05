package solar.mr.io;

import com.spbsu.commons.func.types.ConversionPack;
import com.spbsu.commons.func.types.TypeConverter;
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
      return null;
    }
  }

  public static class From implements TypeConverter<CharSequence, MRTableShard> {
    @Override
    public MRTableShard convert(final CharSequence from) {
      return null;
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
