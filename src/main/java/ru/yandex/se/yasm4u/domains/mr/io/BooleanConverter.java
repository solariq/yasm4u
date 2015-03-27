package ru.yandex.se.yasm4u.domains.mr.io;

import com.spbsu.commons.func.types.ConversionPack;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeqTools;

/**
 * Created by minamoto on 11/03/15.
 */
public class BooleanConverter implements ConversionPack<Boolean,CharSequence> {
  public static class To implements TypeConverter<Boolean, CharSequence> {
    @Override
    public CharSequence convert(Boolean from) {
      return from? "true" : "false";
    }
  }
  public static class From implements TypeConverter<CharSequence, Boolean> {

    @Override
    public Boolean convert(CharSequence from) {
      return !CharSeqTools.toLowerCase(from).equals("false");
    }
  }
  @Override
  public Class<? extends TypeConverter<Boolean, CharSequence>> to() {
    return To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSequence, Boolean>> from() {
    return From.class;
  }
}
