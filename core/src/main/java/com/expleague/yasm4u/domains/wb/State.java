package com.expleague.yasm4u.domains.wb;

import com.expleague.commons.func.types.ConversionRepository;
import com.expleague.commons.func.types.SerializationRepository;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.Set;

import com.expleague.yasm4u.Domain;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public interface State extends Serializable, Domain {
  SerializationRepository<CharSequence> SERIALIZATION = new SerializationRepository<>(
      new SerializationRepository<>(ConversionRepository.ROOT, CharSequence.class),
      "com.expleague.yasm4u.domains.mr.io",
      "com.expleague.yasm4u.domains.wb.io");

  @Nullable
  <T> T get(String uri);

  @Nullable
  <T> T get(StateRef<T> name);

  boolean available(String... consumes);

  boolean available(StateRef... consumes);

  Set<? extends StateRef> keys();

  <T extends StateRef> T parseRef(CharSequence from);
}
