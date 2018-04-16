package com.expleague.yasm4u.impl;

import com.expleague.commons.func.types.TypeConverter;
import com.expleague.yasm4u.Ref;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * User: solar
 * Date: 25.03.15
 * Time: 13:45
 */
public class RefParserImpl implements Ref.Parser {
  private final Map<String, TypeConverter<String, ? extends Ref>> domainConverters = new HashMap<>();

  @Override
  public void registerProtocol(String proto, TypeConverter<String, ? extends Ref> parser) {
    domainConverters.put(proto, parser);
  }

  @Override
  public Ref convert(CharSequence in) {
    final URI uri = URI.create(in.toString());
    final String scheme = uri.getScheme();
    if (scheme == null)
      throw new RuntimeException("Schema is null");
    final TypeConverter<String, ? extends Ref> typeConverter = domainConverters.get(scheme);
    if (typeConverter == null)
      throw new IllegalStateException("Unknown domain: " + scheme);
    return typeConverter.convert(uri.getSchemeSpecificPart());
  }
}
