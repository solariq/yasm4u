package ru.yandex.se.yasm4u.domains.wb.impl;

import com.spbsu.commons.filters.*;
import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqReader;
import ru.yandex.se.yasm4u.*;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.wb.StateRef;
import ru.yandex.se.yasm4u.domains.wb.TempRef;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

import java.util.*;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public class WhiteboardImpl extends StateImpl implements Whiteboard {
  public final static String USER = System.getProperty("mr.user", System.getProperty("user.name"));
  private final Properties increment = new Properties();
  private final MRPath myShard;
  private final MREnv env;
  private SerializationRepository<CharSequence> marshaling;

  public WhiteboardImpl(final MREnv env, final String id) {
    marshaling = new SerializationRepository<>(State.SERIALIZATION).customize(
        new OrFilter<>(
            new AndFilter<TypeConverter>(new ClassFilter<TypeConverter>(Action.class, Whiteboard.class), new Filter<TypeConverter>() {
      @Override
      public boolean accept(final TypeConverter converter) {
        //noinspection unchecked
        ((Action<Whiteboard>) converter).invoke(WhiteboardImpl.this);
        return true;
      }
    }), new TrueFilter<TypeConverter>()));


    myShard = new MRPath(MRPath.Mount.HOME, "state/" + id, false);
    env.read(myShard, new Processor<MRRecord>() {
      @Override
      public void process(final MRRecord arg) {
        try {
          final Object read = marshaling.read(arg.value, marshaling.base.conversionType(Class.forName(arg.sub), CharSequence.class));
          state.put(arg.key, read);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    });
    this.env = env;
  }

  @Override
  public Set<String> keys() {
    final Set<String> keys = new HashSet<>(state.keySet());
    for (Object key : increment.keySet()) {
      keys.add((String) key);
    }
    return keys;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T get(final String resource) {
    final Object value = increment.get(resource);
    if (value != null)
      return value != CharSeq.EMPTY ? (T) value : null;

    return (T)state.get(resource);
  }

  @Override
  public <T> void set(String uri, final T data) {
    if (uri.startsWith("var:"))
      uri = uri.substring("var:".length());
    if (data == CharSeq.EMPTY)
      throw new IllegalArgumentException("User remove instead");
    final Object current = state.get(uri);
    if ((data.getClass().isArray() && Arrays.equals((Object[])data, (Object[])current)) ||data.equals(current))
      increment.remove(uri);
    else
      increment.put(uri, data);
  }

  @Override
  public void remove(final String var) {
    increment.put(var, CharSeq.EMPTY);
  }

  @Override
  public State snapshot() {
    sync();
    return new StateImpl(this);
  }

  @SuppressWarnings("unchecked")
  public void sync() {
    if (increment.isEmpty())
      return;
    // this will update all shards through notification mechanism
    for (Map.Entry<Object, Object> entry : increment.entrySet()) {
      if (CharSeq.EMPTY == entry.getValue())
        //noinspection SuspiciousMethodCalls
        state.remove(entry.getKey());
      else
        state.put((String) entry.getKey(), entry.getValue());
    }

    final CharSeqBuilder builder = new CharSeqBuilder();
    for (Map.Entry<String, Object> entry : state.entrySet()) {
      final Class<?> dataClass = entry.getValue().getClass();
      final TypeConverter<Object, CharSequence> converter = (TypeConverter<Object, CharSequence>)marshaling.base.converter(dataClass, CharSequence.class);
      final Class<?> conversionType = marshaling.base.conversionType(dataClass, CharSequence.class);
      builder.append(entry.getKey()).append('\t');
      builder.append(conversionType.getName()).append('\t');
      builder.append(converter.convert(entry.getValue())).append('\n');
    }

//    if (builder.length() > 0)
    env.write(myShard, new CharSeqReader(builder.build()));
    increment.clear();
  }

  @Override
  public void wipe() {
    sync();
    if (!state.isEmpty()) {
      for (String resourceName : keys()) {
        final Ref<?> ref = Ref.PARSER.convert("var:" + resourceName);
        if (ref instanceof TempRef && MRPath.class.isAssignableFrom(ref.type())) {
          // TODO: remove this dirty hack
          env.delete((MRPath)ref.resolve(new Controller() {
            @Override
            public <T extends Domain> T domain(Class<T> domClass) {
              return (T)env;
            }
            @Override
            public Domain[] domains() {
              return new Domain[]{env};
            }
          }));
        }
      }
      state.clear();
      env.delete(myShard);
    }
  }
}
