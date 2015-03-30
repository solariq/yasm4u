package ru.yandex.se.yasm4u.domains.mr.io;


import com.spbsu.commons.func.types.ConversionPack;
import com.spbsu.commons.func.types.TypeConverter;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.mr.MRPath;

/**
 * User: solar
 * Date: 21.10.14
 * Time: 17:10
 */
@SuppressWarnings("UnusedDeclaration")
public class RefConverter implements ConversionPack<Ref,CharSequence> {
  public static class To implements TypeConverter<Ref, CharSequence> {
    @Override
    public CharSequence convert(final Ref from) {
      return from.toURI().toString();
    }
  }

  public static class From implements TypeConverter<CharSequence, Ref> {
    @Override
    public Ref convert(final CharSequence from) {
      return Ref.PARSER.convert(from.toString());
    }
  }
  @Override
  public Class<? extends TypeConverter<Ref, CharSequence>> to() {
    return To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSequence, Ref>> from() {
    return From.class;
  }
}
