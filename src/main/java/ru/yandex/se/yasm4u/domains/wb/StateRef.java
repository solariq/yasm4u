package ru.yandex.se.yasm4u.domains.wb;

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
  public final String name;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof StateRef)) return false;

    StateRef stateRef = (StateRef) o;

    return name.equals(stateRef.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
