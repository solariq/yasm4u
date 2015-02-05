package solar.mr.io;


import com.spbsu.commons.func.types.ConversionPack;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.MRTableState;
import solar.mr.proc.impl.MRPath;

/**
 * User: solar
 * Date: 21.10.14
 * Time: 17:10
 */
@SuppressWarnings("UnusedDeclaration")
public class MRPathConverter implements ConversionPack<MRPath,CharSequence> {
  public static class To implements TypeConverter<MRPath, CharSequence> {
    @Override
    public CharSequence convert(final MRPath from) {
      return from.toString();
    }
  }

  public static class From implements TypeConverter<CharSequence, MRPath> {
    @Override
    public MRPath convert(final CharSequence from) {
      return MRPath.create(from.toString());
    }
  }
  @Override
  public Class<? extends TypeConverter<MRPath, CharSequence>> to() {
    return To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSequence, MRPath>> from() {
    return From.class;
  }
}
