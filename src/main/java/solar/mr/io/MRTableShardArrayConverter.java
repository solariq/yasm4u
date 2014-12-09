package solar.mr.io;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.types.ConversionPack;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.MRTableShard;
import solar.mr.proc.MRWhiteboard;

import java.util.ArrayList;

/**
 * Created by inikifor on 09.12.14.
 */
public class MRTableShardArrayConverter implements ConversionPack<MRTableShard[],CharSequence> {

  public static final class ToArray implements TypeConverter<MRTableShard[], CharSequence> {

    private final MRTableShardConverter.To SINGLE_TO = new MRTableShardConverter.To();

    @Override
    public CharSequence convert(final MRTableShard[] from) {
      final StringBuilder builder = new StringBuilder();
      for(MRTableShard shard: from) {
        builder.append(SINGLE_TO.convert(shard));
        builder.append("@");
      }
      return builder.toString();
    }
  }

  public static class FromArray implements TypeConverter<CharSequence, MRTableShard[]>, Action<MRWhiteboard> {

    private final MRTableShardConverter.From SINGLE_FROM = new MRTableShardConverter.From();

    @Override
    public void invoke(final MRWhiteboard wb) {
      SINGLE_FROM.invoke(wb);
    }

    @Override
    public MRTableShard[] convert(final CharSequence from) {
      CharSequence[] parts = CharSeqTools.split(from, "@");
      ArrayList<MRTableShard> result = new ArrayList();
      for(CharSequence part: parts) {
        if (part.length() > 0) {
          SINGLE_FROM.convert(part);
        }
      }
      return result.toArray(new MRTableShard[0]);
    }
  }

  @Override
  public Class<? extends TypeConverter<MRTableShard[], CharSequence>> to() {
    return ToArray.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSequence, MRTableShard[]>> from() {
    return FromArray.class;
  }
}
