package ru.yandex.se.yasm4u.domains.wb;

import com.spbsu.commons.random.FastRandom;
import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.wb.impl.WhiteboardImpl;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * User: solar
 * Date: 27.03.15
 * Time: 11:37
 */
public class TempRef<T> extends StateRef<T> {
  private final Class<T> type;
  private final Domain.Controller controller;

  public TempRef(String name, Class<T> type, Domain.Controller controller) {
    super(name, type);
    this.type = type;
    this.controller = controller;
  }

  @Override
  public URI toURI() {
    try {
      return new URI("temp:" + name);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Class<T> type() {
    return type;
  }

  @Override
  public Class<State> domainType() {
    return State.class;
  }

  @Override
  public T resolve(State wb) {
    return controller.resolve(realRef(wb));
  }

  @Override
  public boolean available(State wb) {
    return controller.available(realRef(wb));
  }

  private Ref<T, ?> realRef(State state) {
    @SuppressWarnings("unchecked")
    Ref<T, ?> result = (Ref<T, ?>)state.get(this);
    if (result == null) {
      if (name.startsWith("mr://")) {
        final MRPath path = MRPath.createFromURI(name);
        //noinspection unchecked
        result = (Ref<T, ?>)new MRPath(MRPath.Mount.TEMP, WhiteboardImpl.USER + "/" + path.path + "-" + Integer.toHexString(new FastRandom().nextInt()), path.sorted);
        ((Whiteboard)state).set(this, result);
      }
      else throw new UnsupportedOperationException("Unknown schema for temp allocation: " + name);
    }
    return result;
  }
}
