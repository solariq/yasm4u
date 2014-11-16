package solar.mr.proc.impl;

import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;


import com.spbsu.commons.filters.*;
import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.system.RuntimeUtils;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.routines.MRRecord;
import solar.mr.proc.MRState;
import solar.mr.proc.MRWhiteboard;
import solar.mr.MRTableShard;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public class MRWhiteboardImpl implements MRWhiteboard, Action<MREnv.ShardAlter> {
  public static final long FRESHNESS_TIMEOUT = TimeUnit.HOURS.toMillis(1);
  private final MREnv env;
  private final String user;
  private final MRStateImpl state = new MRStateImpl();
  private final Properties increment = new Properties();
  private final MRTableShard myShard;
  private final Random rng = new FastRandom();
  private MRErrorsHandler errorsHandler;
  private SerializationRepository<CharSequence> marshaling;
  private MRWhiteboard connected;

  public MRWhiteboardImpl(final MREnv env, final String id, final String user) {
    this(env, id, user, new MRErrorsHandler() {
      @Override
      public void error(final String type, final String cause, final MRRecord rec) {
        System.err.println(rec.source + "\t" + type + "\t" + cause);
        System.err.println(rec.toString());
      }

      @Override
      public void error(final Throwable th, final MRRecord rec) {
        System.err.print(rec.source + "\t");
        th.printStackTrace(System.err);
        System.err.println(rec.toString());
      }
    });
  }

  public MRWhiteboardImpl(final MREnv env, final String id, final String user, MRErrorsHandler handler) {
    marshaling = new SerializationRepository<>(MRState.SERIALIZATION).customize(
        new OrFilter<>(
            new AndFilter<TypeConverter>(new ClassFilter<TypeConverter>(Action.class, MRWhiteboard.class), new Filter<TypeConverter>() {
      @Override
      public boolean accept(final TypeConverter converter) {
        //noinspection unchecked
        ((Action<MRWhiteboard>) converter).invoke(MRWhiteboardImpl.this);
        return true;
      }
    }), new TrueFilter<TypeConverter>()));

    errorsHandler = handler;
    this.env = env;
    this.user = user;
    myShard = new LazyTableShard("temp/" + user + "/state/" + id, env);
    env.read(myShard, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        if (arg.length() == 0)
          return;
        CharSequence[] parts = CharSeqTools.split(arg, '\t');
        try {
          if (parts[1].length() > 0)
            state.state.put(parts[0].toString(), marshaling.read(parts[2], Class.forName(parts[1].toString())));
          else
            state.state.remove(parts[0].toString());
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    });
    env.addListener(this);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T resolve(final String resource) {
    try {
      if (resource.indexOf(':') < 0)
        return (T) resource;
      else if (state.state.containsKey(resource))
        return state.get(resource);
      else if (increment.containsKey(resource))
        return (T)increment.get(resource);

      final URI uri = new URI(resource);
      final String scheme = uri.getScheme();
      switch(scheme) {
        case "temp":
          final String subProtocol = uri.getSchemeSpecificPart();
          if (subProtocol.startsWith("mr://")) {
            final String path = "temp/" + user + subProtocol.substring("mr://".length()) + "-" + (Integer.toHexString(rng.nextInt()));
            final MRTableShard resolve = new LazyTableShard(path, env);
            set(resource, resolve);
            return (T)resolve;
          }
          else throw new UnsupportedOperationException("Unknown schema for temp allocation: " + subProtocol);
        case "mr":
          final MRTableShard resolve = env.resolve(uri.getPath().substring(1));
          set(resource, resolve);
          return (T)resolve;
        case "var":
          return state.get(resource);
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
  public void setErrorsHandler(final MRErrorsHandler errorsHandler) {
    this.errorsHandler = errorsHandler;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> boolean processAs(String name, Processor<T> processor) {
    final Method[] methods = processor.getClass().getMethods();
    Class<?> clz = null;
    for(int i = 0; i < methods.length; i++) {
      final Method m = methods[i];
      if (!m.isSynthetic() && "process".equals(m.getName()) && m.getReturnType() == void.class && m.getParameterTypes().length == 1) {
        clz = m.getParameterTypes()[0];
      }
    }
    if (clz == null)
      throw new IllegalArgumentException("Unable to infer type parameter from processor: " + processor.getClass().getName());
    final Object resolve = resolve(name);

    if (!clz.isAssignableFrom(resolve.getClass()))
      return false;
    processor.process((T)resolve);
    return true;
  }

  @Override
  public SerializationRepository marshaling() {
    return marshaling;
  }

  @Override
  public <T> void set(final String uri, final T data) {
    if (data.equals(state.state.get(uri)))
      return;
    increment.put(uri, data);
  }

  @Override
  public void remove(final String var) {
    increment.put(var, "");
    env.append(myShard, new CharSeqReader(var + "\t\n"));
  }

  @Override
  public MRState snapshot() {
    sync();
    return new MRStateImpl(state);
  }

  @Override
  public boolean check(final String... productName) {
    for (String resource : productName) {
      final Object resolve = resolve(resource);
      if (resolve == null || ((resolve instanceof MRTableShard) && !((MRTableShard) resolve).isAvailable()))
        return false;
    }
    return true;
  }

  private final Set<String> hints = new HashSet<>();

  @Override
  public void invoke(MREnv.ShardAlter shardAlter) {
    final String path = shardAlter.shard.path();
    switch (shardAlter.type) {
      case CHANGED:
        hints.add(path);
        break;
      case UPDATED:
        hints.remove(path);
        for (Map.Entry<Object, Object> entry : increment.entrySet()) {
          if (entry.getValue() instanceof MRTableShard && path.equals(((MRTableShard) entry.getValue()).path()))
            set((String)entry.getKey(), shardAlter.shard);
        }
        for (Map.Entry<String, Object> entry : state.state.entrySet()) {
          if (entry.getValue() instanceof MRTableShard && path.equals(((MRTableShard) entry.getValue()).path()))
            set(entry.getKey(), shardAlter.shard);
        }
        break;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void sync() {
    final long currentTime = System.currentTimeMillis();
    final List<String> shards = new ArrayList<>();
    for (final String resourceName : state.available()) {
      processAs(resourceName, new Processor<MRTableShard>() {
        @Override
        public void process(final MRTableShard shard) {
          if (hints.contains(shard.path()) || currentTime - shard.metaTS() > FRESHNESS_TIMEOUT) {
            shards.add(shard.path());
          }
        }
      });
    }
    env.resolveAll(shards.toArray(new String[shards.size()]));

    final CharSeqBuilder builder = new CharSeqBuilder();
    for (Map.Entry<Object, Object> entry : increment.entrySet()) {
      final Class<?> dataClass = entry.getValue().getClass();
      final TypeConverter<Object, CharSequence> converter = (TypeConverter<Object, CharSequence>)marshaling.base.converter(dataClass, CharSequence.class);
      final Class[] typeParameters = RuntimeUtils.findTypeParameters(converter.getClass(), TypeConverter.class);
      builder.append((String)entry.getKey()).append('\t');
      builder.append(typeParameters[0] != null ? typeParameters[0].getName() : dataClass.getName()).append('\t');
      builder.append(converter.convert(entry.getValue())).append('\n');
      state.state.put((String)entry.getKey(), entry.getValue());
    }
    if (builder.length() > 0)
      env.write(myShard, new CharSeqReader(builder.build()));
    increment.clear();
  }

  @Override
  public void wipe() {
    sync();
    for (String resourceName : state.available()) {
      final Object resource = state.get(resourceName);
      if (resourceName.startsWith("temp:")) {
        if (resource instanceof MRTableShard) {
          env.delete((MRTableShard) resource);
        }
        else {
          throw new RuntimeException("Unknown temporary resource: " + resourceName);
        }
      }
    }
    env.delete(myShard);
    if (connected != null)
      connected.wipe();
  }

  public void connect(final MRWhiteboard test) {
    connected = test;
  }

  private static class LazyTableShard extends MRTableShard {
    MRTableShard realShard;

    public LazyTableShard(String path, MREnv env) {
      super(path, env, false, false, "");
    }

    private MRTableShard real() {
      if (realShard == null)
        realShard = this.container().resolve(path());
      return realShard;
    }

    @Override
    public boolean isAvailable() {
      return real().isAvailable();
    }

    @Override
    public boolean isSorted() {
      return real().isSorted();
    }

    @Override
    public String crc() {
      return real().crc();
    }

    @Override
    public long metaTS() {
      return real().metaTS();
    }
  }
}
