package ru.yandex.se.yasm4u.domains.wb;

import com.spbsu.commons.func.Processor;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.Set;


import com.spbsu.commons.func.types.ConversionRepository;
import com.spbsu.commons.func.types.SerializationRepository;
import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.Ref;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public interface State extends Serializable, Domain {
  SerializationRepository<CharSequence> SERIALIZATION = new SerializationRepository<>(
      new SerializationRepository<>(ConversionRepository.ROOT, CharSequence.class),
      "ru.yandex.se.yasm4u.domains.mr.io");

  @Nullable
  <T> T get(String uri);

  boolean available(String... consumes);
  Set<String> keys();
  <T> boolean processAs(String name, Processor<T> processor);
}
