package solar.mr.io;


import com.spbsu.commons.func.converters.ArrayConverters;
import com.spbsu.commons.func.types.ConversionPack;
import com.spbsu.commons.func.types.TypeConverter;
import solar.mr.proc.impl.MRPath;

/**
 * User: solar
 * Date: 21.10.14
 * Time: 17:10
 */
@SuppressWarnings("UnusedDeclaration")
public class MRPathArrayConverter implements ConversionPack<MRPath[],CharSequence> {
  public static class To extends ArrayConverters.To<MRPath> {
  }

  public static class From extends ArrayConverters.From<MRPath> {
  }
  @Override
  public Class<? extends TypeConverter<MRPath[], CharSequence>> to() {
    return To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSequence, MRPath[]>> from() {
    return From.class;
  }
}
