package com.expleague.yasm4u.domains.mr.io;

import com.expleague.commons.func.types.ConversionPack;
import com.expleague.commons.func.types.TypeConverter;
import com.expleague.yasm4u.domains.mr.MRPath;

/**
 * User: solar
 * Date: 21.10.14
 * Time: 17:10
 */
@SuppressWarnings("UnusedDeclaration")
public class MRPathConverter implements ConversionPack<MRPath, CharSequence> {
  public static class To implements TypeConverter<MRPath, CharSequence> {
    @Override
    public CharSequence convert(final MRPath from) {
      return from.toURI().toString();
    }
  }

  public static class From implements TypeConverter<CharSequence, MRPath>{
    @Override
    public MRPath convert(final CharSequence from) {
      return MRPath.createFromURI(from.toString());
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
