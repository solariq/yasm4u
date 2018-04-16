package com.expleague.yasm4u;

import com.expleague.commons.func.types.TypeConverter;

import java.net.URI;

/**
 * User: solar
 * Date: 16.03.15
 * Time: 15:40
 */
public interface Ref<T, D extends Domain> {
  URI toURI();
  Class<T> type();
  Class<D> domainType();

  T resolve(D controller);
  boolean available(D controller);

  int hashCode();
  boolean equals(Object o);

  interface Parser extends TypeConverter<CharSequence, Ref> {
    void registerProtocol(String proto, TypeConverter<String, ? extends Ref> parser);
  }
}
