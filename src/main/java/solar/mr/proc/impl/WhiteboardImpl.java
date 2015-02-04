package solar.mr.proc.impl;

import com.spbsu.commons.filters.*;
import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.system.RuntimeUtils;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.proc.State;
import solar.mr.proc.Whiteboard;
import solar.mr.routines.MRRecord;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public class WhiteboardImpl extends StateImpl implements Whiteboard {

  public final static String USER = System.getProperty("mr.user", System.getProperty("user.name"));
  private final MREnv env;
  private final String user;
  private final Properties increment = new Properties();
  private final MRPath myShard;
  private final Random rng = new FastRandom();
  private MRErrorsHandler errorsHandler;
  private SerializationRepository<CharSequence> marshaling;

  public WhiteboardImpl(final MREnv env, final String id, final String user) {
    this(env, id, user, new MRErrorsHandler() {
      int counter = 0;
      @Override
      public void error(final String type, final String cause, final MRRecord rec) {
        System.err.println(rec.source + "\t" + type + "\t" + cause);
        System.err.println(rec.toString());
        counter++;
      }

      @Override
      public void error(final Throwable th, final MRRecord rec) {
        System.err.print(rec.source + "\t");
        th.printStackTrace(System.err);
        System.err.println(rec.toString());
        counter++;
      }

      @Override
      public int errorsCount() {
        return counter;
      }
    });
  }

  public WhiteboardImpl(final MREnv env, final String id, final String user, MRErrorsHandler handler) {
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

    errorsHandler = handler;
    this.env = env;
    this.user = user;

    myShard = new MRPath(MRPath.Mount.HOME, "state/" + id, false);
    env.read(myShard, new Processor<MRRecord>() {
      @Override
      public void process(final MRRecord arg) {
        try {
          state.put(arg.key, marshaling.read(arg.sub, Class.forName(arg.value.toString())));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  public WhiteboardImpl(MREnv env, String name) {
    this(env, name, USER);
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
    try {
      final Object value = increment.get(resource);
      if (value != null && value != CharSeq.EMPTY)
        return (T) value;
      if (state.containsKey(resource))
        return (T)state.get(resource);

      final URI uri = new URI(resource);
      final String scheme = uri.getScheme();
      if (scheme == null) {
        throw new RuntimeException("Schema is null");
      }
      switch(scheme) {
        case "temp":
          final String subProtocol = uri.getSchemeSpecificPart();
          if (subProtocol.startsWith("mr://")) {
            int offset = "mr://".length();
            final String realPath = "mr:///tmp/"+ user + subProtocol.substring(offset) + "-" + (Integer.toHexString(rng.nextInt()));
            return get(realPath);
          }
          else throw new UnsupportedOperationException("Unknown schema for temp allocation: " + subProtocol);
        case "mr":
          Object result;
          final String path = uri.getPath();
          if (resource.endsWith("*"))
            result = env.list(MRPath.create(path.substring(0, path.length() - 1)));
          else
            result = MRPath.create(path);
          set(resource, result);
          return (T)result;
      }
      return null;
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public MREnv env() {
    return env;
  }

  @Override
  public MRErrorsHandler errorsHandler() {
    return errorsHandler;
  }

  @Override
  public <T> void set(final String uri, final T data) {
    if (data == CharSeq.EMPTY)
      throw new IllegalArgumentException("User remove instead");
    if (!data.equals(state.get(uri)))
      increment.put(uri, data);
  }

  @Override
  public void remove(final String var) {
    increment.put(var, CharSeq.EMPTY);
    env.append(myShard, new CharSeqReader(var + "\t\n"));
  }

  @Override
  public State snapshot() {
    sync();
    return new StateImpl(this);
  }

  @SuppressWarnings("unchecked")
  public void sync() {
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
      final Class[] typeParameters = RuntimeUtils.findTypeParameters(converter.getClass(), TypeConverter.class);
      builder.append(entry.getKey()).append('\t');
      builder.append(typeParameters[0] != null ? typeParameters[0].getName() : dataClass.getName()).append('\t');
      builder.append(converter.convert(entry.getValue())).append('\n');
    }

    if (builder.length() > 0)
      env.write(myShard, new CharSeqReader(builder.build()));
    increment.clear();
  }

  @Override
  public void wipe() {
    sync();
    if (!state.isEmpty()) {
      for (String resourceName : keys()) {
        if (resourceName.startsWith("temp:")) {
          if (!processAs(resourceName, new Processor<MRPath>() {
            @Override
            public void process(MRPath arg) {
              env.delete(arg);
            }
          }))
            throw new RuntimeException("Unknown temporary resource type: " + resourceName);
        }
      }
      env.delete(myShard);
    }
  }

  public void setErrorsHandler(MRErrorsHandler errorsHandler) {
    this.errorsHandler = errorsHandler;
  }
}
