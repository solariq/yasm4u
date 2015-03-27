package ru.yandex.se.yasm4u;

import com.spbsu.commons.func.types.TypeConverter;
import ru.yandex.se.yasm4u.impl.RefParserImpl;

import java.net.URI;

/**
 * User: solar
 * Date: 16.03.15
 * Time: 15:40
 */
public interface Ref<T> {
  Parser PARSER = new RefParserImpl();

  URI toURI();
  Class<T> type();
  Class<? extends Domain> domainType();

  T resolve(Domain.Controller controller);
  boolean available(Domain.Controller controller);

  interface Parser extends TypeConverter<String, Ref<?>> {
    void registerProtocol(String proto, TypeConverter<String, Ref<?>> parser);
  }
}
