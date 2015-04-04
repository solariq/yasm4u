package ru.yandex.se.yasm4u.domains.wb.impl;

import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.TypeConverter;
import org.jetbrains.annotations.Nullable;
import ru.yandex.se.yasm4u.JobExecutorService;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRTableState;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.wb.StateRef;
import ru.yandex.se.yasm4u.domains.wb.TempRef;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public class StateImpl implements State {
  // never ever use this variable out of impl package!!!!!!!!
  protected Map<String, Object> state = new HashMap<>();

  public StateImpl() {}

  public StateImpl(State copy) {
    for (final String key : copy.keys()) {
      state.put(key, copy.get(key));
    }
  }

  @Nullable
  @Override
  public <T> T get(final String name) {
    //noinspection unchecked
    if (!state.containsKey(name))
      return null;

    //noinspection unchecked
    return (T)state.get(name);
  }

  @Override
  public boolean available(final String... consumes) {
    final boolean[] holder = new boolean[]{true};
    for (int i = 0; holder[0] && i < consumes.length; i++) {
      if (!processAs(consumes[i], new Processor<MRTableState>() {
        @Override
        public void process(MRTableState shard) {
          holder[0] &= shard.isAvailable();
        }
      }))
        holder[0] &= keys().contains(consumes[i]);
    }
    return holder[0];
  }

  @Override
  public Set<String> keys() {
    return state.keySet();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> boolean processAs(String name, Processor<T> processor) {
    Class<?> clz = null;
    {
      final Method[] methods = processor.getClass().getMethods();
      for (int i = 0; i < methods.length; i++) {
        final Method m = methods[i];
        if (!m.isSynthetic() && "process".equals(m.getName()) && m.getReturnType() == void.class && m.getParameterTypes().length == 1) {
          clz = m.getParameterTypes()[0];
        }
      }
      if (clz == null)
        throw new IllegalArgumentException("Unable to infer type parameter from processor: " + processor.getClass().getName());
    }
    final Object resolve = get(name);

    if (resolve == null)
      return false;
    final Class<?> resolveClass = resolve.getClass();
    if (resolveClass.isArray() && clz.isAssignableFrom(resolveClass.getComponentType())) {
      final T[] arr = (T[])resolve;
      for(int i = 0; i < arr.length; i++) {
        final T element = arr[i];
        processor.process(element);
      }
    }
    else if (clz.isAssignableFrom(resolveClass))
      processor.process((T)resolve);
    else
      return false;
    return true;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    for(final String current : keys()) {
      final Object instance = get(current);
      assert instance != null;
      final SerializationRepository<CharSequence> serialization = State.SERIALIZATION;
      final Class conversionType  = serialization.base.conversionType(instance.getClass(), CharSequence.class);
      out.writeBoolean(true);
      out.writeUTF(current);
      out.writeUTF(conversionType.getName());
      out.writeUTF(serialization.write(instance).toString());
    }
    out.writeBoolean(false);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    state = new HashMap<>();
    while(in.readBoolean()) {
      final String current = in.readUTF();
      final String itemClass = in.readUTF();
      final Object read = State.SERIALIZATION.read(in.readUTF(), Class.forName(itemClass));
      state.put(current, read);
    }
  }

  private void readObjectNoData() throws ObjectStreamException {
    state = new HashMap<>();
  }

  @Override
  public void init(JobExecutorService jes) {
    jes.addJoba(new PublisherJoba(StateImpl.this));
  }

  static {
    Ref.PARSER.registerProtocol("var", new TypeConverter<String, Ref<?>>() {
      @Override
      public Ref<?> convert(final String from) {
        //noinspection unchecked
        return new StateRef(from, Object.class);
      }
    });
    Ref.PARSER.registerProtocol("temp", new TypeConverter<String, Ref<?>>() {
      @Override
      public Ref<?> convert(String from) {
        return new TempRef<>(from, Object.class);
      }
    });
  }
}
