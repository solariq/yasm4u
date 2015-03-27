package ru.yandex.se.yasm4u.domains.wb;

import com.spbsu.commons.func.types.TypeConverter;
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
public class TempRef<T> implements Ref<T> {
  public final Ref<T> realRef;

  public TempRef(Ref<T> realRef) {
    this.realRef = realRef;
  }

  @Override
  public URI toURI() {
    try {
      return new URI("temp:" + realRef.toURI().toString());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Class<T> type() {
    return realRef.type();
  }

  @Override
  public Class<? extends Domain> domainType() {
    return realRef.domainType();
  }

  @Override
  public T resolve(Domain.Controller controller) {
    return realRef.resolve(controller);
  }

  @Override
  public boolean available(Domain.Controller controller) {
    return realRef.available(controller);
  }

  static {
    Ref.PARSER.registerProtocol("temp", new TypeConverter<String, Ref<?>>() {
      @Override
      public Ref<?> convert(String from) {
        if (from.startsWith("mr://")) {
          final MRPath path = MRPath.createFromURI(from);
          final MRPath result = new MRPath(MRPath.Mount.TEMP, WhiteboardImpl.USER + "/" + path.path + "-" + Integer.toHexString(new FastRandom().nextInt()), path.sorted);
          return new TempRef<>(result);
        }
        else throw new UnsupportedOperationException("Unknown schema for temp allocation: " + from);
      }
    });
  }
}
