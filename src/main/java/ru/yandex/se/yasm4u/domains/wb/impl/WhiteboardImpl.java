package ru.yandex.se.yasm4u.domains.wb.impl;

import com.spbsu.commons.filters.*;
import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.util.ArrayTools;
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.wb.StateRef;
import ru.yandex.se.yasm4u.domains.wb.TempRef;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;
import ru.yandex.se.yasm4u.impl.RefParserImpl;

import java.util.*;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public class WhiteboardImpl extends StateImpl implements Whiteboard {
  public final static String USER = System.getProperty("mr.user", System.getProperty("user.name"));
  private final Map<StateRef, Object> increment = new HashMap<>();
  private final MRPath myShard;
  private final MREnv env;
  private SerializationRepository<CharSequence> marshaling;

  public WhiteboardImpl(final MREnv env, final String id) {
    super(env);
    final RefParserImpl parser = new RefParserImpl();
    publishReferenceParsers(parser, trash);
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
          state.put((StateRef) parser.convert(arg.key), read);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    });
    this.env = env;
  }

  @Override
  public Set<? extends StateRef> keys() {
    final Set<StateRef> keys = new HashSet<>(state.keySet());
    for (final StateRef key : increment.keySet()) {
      keys.add(key);
    }
    return keys;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T get(final StateRef<T> resource) {
    final Object value = increment.get(resource);
    if (value != null)
      return value != CharSeq.EMPTY ? (T) value : null;

    return (T)state.get(resource);
  }

  @Override
  public <T> void set(String uri, final T data) {
    set(parseRef(uri), data);
  }

  @Override
  public <T> void set(StateRef uri, final T data) {
    if (data == CharSeq.EMPTY)
      throw new IllegalArgumentException("User remove instead");
    final Object current = state.get(uri);
    if (data.equals(current) || (data.getClass().isArray() && Arrays.equals((Object[]) data, (Object[]) current)))
      increment.remove(uri);
    else
      increment.put(uri, data);
  }

  @Override
  public void remove(final String var) {
    remove(parseRef(var));
  }

  @Override
  public void remove(final StateRef var) {
    increment.put(var, CharSeq.EMPTY);
  }

  @Override
  public boolean available(StateRef... consumes) {
    return super.available(consumes) || ArrayTools.and(consumes, new Filter<StateRef>() {
      @Override
      public boolean accept(StateRef stateRef) {
        return increment.containsKey(stateRef);
      }
    });
  }

  @Override
  public State snapshot() {
    sync();
    return new StateImpl(this, trash);
  }

  @SuppressWarnings("unchecked")
  public void sync() {
    if (increment.isEmpty())
      return;
    // this will update all shards through notification mechanism
    for (Map.Entry<StateRef, Object> entry : increment.entrySet()) {
      if (CharSeq.EMPTY == entry.getValue())
        //noinspection SuspiciousMethodCalls
        state.remove(entry.getKey());
      else
        state.put(entry.getKey(), entry.getValue());
    }

    final CharSeqBuilder builder = new CharSeqBuilder();
    for (Map.Entry<StateRef, Object> entry : state.entrySet()) {
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
      for (StateRef ref : keys()) {
        if (ref instanceof TempRef && MRPath.class.isAssignableFrom(ref.type())) {
          // TODO: remove this dirty hack
          env.delete((MRPath) ref.resolve(this));
        }
      }
      state.clear();
      env.delete(myShard);
    }
  }
}
