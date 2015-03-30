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

  public TempRef(String name, Class<T> type) {
    super(name, type);
    this.type = type;
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
  public Class<? extends Domain> domainType() {
    return State.class;
  }

  @Override
  public T resolve(Domain.Controller controller) {
    return realRef(controller).resolve(controller);
  }

  @Override
  public boolean available(Domain.Controller controller) {
    return realRef(controller).available(controller);
  }

  private Ref<T> realRef(Domain.Controller controller) {
    Ref<T> result = controller.domain(State.class).get(name);
    if (result == null) {
      if (name.startsWith("mr://")) {
        final MRPath path = MRPath.createFromURI(name);
        //noinspection unchecked
        result = (Ref<T>)new MRPath(MRPath.Mount.TEMP, WhiteboardImpl.USER + "/" + path.path + "-" + Integer.toHexString(new FastRandom().nextInt()), path.sorted);
      }
      else throw new UnsupportedOperationException("Unknown schema for temp allocation: " + name);
    }
    return result;
  }
}
