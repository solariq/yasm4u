package solar.mr.io;

import com.spbsu.commons.func.Converter;
import com.spbsu.commons.func.types.ConversionPack;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqTools;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by minamoto on 19/11/14.
 */
public class StringArray2CharSequenceConverter implements ConversionPack<String[], CharSequence> {
  private final static String SEP = ";";
  @Override
  public Class<? extends TypeConverter<String[], CharSequence>> to() {
    return To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSequence, String[]>> from() {
    return From.class;
  }

  public static class To implements TypeConverter<String[], CharSequence> {

    @Override
    public CharSequence convert(String[] from) {

      final CharSeqBuilder builder = new CharSeqBuilder();
      for (int i = 0; i < from.length; ++i) {
        builder.append(CharSeqTools.toBase64(from[i].getBytes(Charset.forName("UTF-8"))));
        if (i < from.length - 1)
          builder.append(SEP);
      }
      return builder.build();
    }
  }

  public static class From implements TypeConverter<CharSequence, String[]> {
    @Override
    public String[] convert(CharSequence from) {
      final CharSequence[] split = CharSeqTools.split(from, SEP);
      final String[] result = new String[split.length];
      for(int i = 0; i < split.length; ++i)
        result[i] = new String(CharSeqTools.parseBase64(split[i]), Charset.forName("UTF-8"));
      return result;
    }
  }
}
