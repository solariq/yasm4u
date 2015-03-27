package ru.yandex.se.yasm4u.domains.wb;

import com.spbsu.commons.func.types.TypeConverter;
import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.Ref;

import java.net.URI;
import java.net.URISyntaxException;

/**
* User: solar
* Date: 26.03.15
* Time: 18:06
*/
public class StateRef<T> implements Ref<T> {
  private final String name;
  private final Class<T> clazz;

  public StateRef(String name, Class<T> clazz) {
    this.name = name;
    this.clazz = clazz;
  }

  @Override
  public URI toURI() {
    try {
      return new URI("var:" + name);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Class<T> type() {
    return clazz;
  }

  @Override
  public Class<? extends Domain> domainType() {
    return State.class;
  }

  @Override
  public T resolve(Domain.Controller controller) {
    return controller.domain(State.class).get(name);
  }

  @Override
  public boolean available(Domain.Controller controller) {
    return controller.domain(State.class).available(name);
  }

  static {
    Ref.PARSER.registerProtocol("var", new TypeConverter<String, Ref<?>>() {
      @Override
      public Ref<?> convert(final String from) {
        //noinspection unchecked
        return new StateRef(from, Object.class);
      }
    });
  }
}
