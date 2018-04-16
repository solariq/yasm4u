package com.expleague.yasm4u.domains.mr.io;

import com.expleague.commons.func.types.ConversionPack;
import com.expleague.commons.func.types.TypeConverter;
import com.expleague.commons.seq.CharSeqBuilder;
import com.expleague.commons.seq.CharSeqTools;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

/**
 * Created by minamoto on 13/02/15.
 */
public class URI2CharSequenceConverter implements ConversionPack<URI, CharSequence> {
  @Override
  public Class<? extends TypeConverter<URI, CharSequence>> to() {
    return To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSequence, URI>> from() {
    return From.class;
  }

  public static class To implements TypeConverter<URI, CharSequence> {

    @Override
    public CharSequence convert(URI from) {
      final CharSeqBuilder builder = new CharSeqBuilder();
      builder.append(CharSeqTools.toBase64(from.toString().getBytes(Charset.forName("UTF-8"))));
      return builder.build();
    }
  }

  public static class From implements TypeConverter<CharSequence, URI> {
    @Override
    public URI convert(CharSequence from) {
      try {
        return new URI(new String(CharSeqTools.parseBase64(from), Charset.forName("UTF-8")));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
