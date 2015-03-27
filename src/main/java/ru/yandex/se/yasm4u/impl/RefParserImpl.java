package ru.yandex.se.yasm4u.impl;

import com.spbsu.commons.func.types.TypeConverter;
import ru.yandex.se.yasm4u.Ref;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * User: solar
 * Date: 25.03.15
 * Time: 13:45
 */
public class RefParserImpl implements Ref.Parser {
  private final Map<String, TypeConverter<String, Ref<?>>> domainConverters = new HashMap<>();

  @Override
  public void registerProtocol(String proto, TypeConverter<String, Ref<?>> parser) {
    domainConverters.put(proto, parser);
  }

  @Override
  public Ref<?> convert(String in) {
    final URI uri = URI.create(in);
    final String scheme = uri.getScheme();
    if (scheme == null)
      throw new RuntimeException("Schema is null");
    final TypeConverter<String, Ref<?>> typeConverter = domainConverters.get(scheme);
    if (typeConverter == null)
      throw new IllegalStateException("Unknown domain: " + scheme);
    return typeConverter.convert(uri.getSchemeSpecificPart());
  }
}
