package solar.mr.io;

import com.spbsu.commons.func.types.ConversionPack;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.MRTableState;

import java.util.ArrayList;

/**
 * Created by inikifor on 09.12.14.
 */
public class MRTableShardArrayConverter implements ConversionPack<MRTableState[],CharSequence> {

  public static final class ToArray implements TypeConverter<MRTableState[], CharSequence> {

    private final MRTableShardConverter.To SINGLE_TO = new MRTableShardConverter.To();

    @Override
    public CharSequence convert(final MRTableState[] from) {
      final StringBuilder builder = new StringBuilder();
      for(MRTableState shard: from) {
        builder.append(SINGLE_TO.convert(shard));
        builder.append("@");
      }
      return builder.toString();
    }
  }

  public static class FromArray implements TypeConverter<CharSequence, MRTableState[]> {

    private final MRTableShardConverter.From SINGLE_FROM = new MRTableShardConverter.From();

    @Override
    public MRTableState[] convert(final CharSequence from) {
      CharSequence[] parts = CharSeqTools.split(from, "@");
      ArrayList<MRTableState> result = new ArrayList();
      for(CharSequence part: parts) {
        if (part.length() > 0) {
          SINGLE_FROM.convert(part);
        }
      }
      return result.toArray(new MRTableState[0]);
    }
  }

  @Override
  public Class<? extends TypeConverter<MRTableState[], CharSequence>> to() {
    return ToArray.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSequence, MRTableState[]>> from() {
    return FromArray.class;
  }
}
