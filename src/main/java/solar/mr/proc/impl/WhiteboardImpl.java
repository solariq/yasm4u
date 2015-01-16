package solar.mr.proc.impl;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


import com.spbsu.commons.filters.*;
import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.system.RuntimeUtils;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.MRTools;
import solar.mr.proc.State;
import solar.mr.proc.Whiteboard;
import solar.mr.routines.MRRecord;
import solar.mr.MRTableShard;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public class WhiteboardImpl extends StateImpl implements Whiteboard, Action<MREnv.ShardAlter> {
  private final MREnv env;
  private final String user;
  private final Properties increment = new Properties();
  private final MRTableShard myShard;
  private final Random rng = new FastRandom();
  private MRErrorsHandler errorsHandler;
  private SerializationRepository<CharSequence> marshaling;

  public WhiteboardImpl(final MREnv env, final String id, final String user) {
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

    myShard = new LazyTableShard(env.getTmp() + user + "/state/" + id, env);
    env.read(myShard, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        if (arg.length() == 0)
          return;
        CharSequence[] parts = CharSeqTools.split(arg, '\t');
        try {
          if (parts.length > 1) {
            if (parts[1].length() > 0)
              state.put(parts[0].toString(), marshaling.read(parts[2], Class.forName(parts[1].toString())));
            else
              state.remove(parts[0].toString());
          }
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    });
    env.addListener(this);
  }

  public WhiteboardImpl(MREnv env, String name) {
    this(env, name, System.getenv("USER"));
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
            final String path = env.getTmp() + user + subProtocol.substring(offset) + "-" + (Integer.toHexString(rng.nextInt()));
            final MRTableShard resolve = new LazyTableShard(path, env);
            set(resource, resolve);
            return (T)resolve;
          }
          else throw new UnsupportedOperationException("Unknown schema for temp allocation: " + subProtocol);
        case "mr":
          Object result;
          final String path = uri.getPath();
          if (resource.endsWith("*"))
            result = env.list(path.substring(0, path.length() - 1));
          else {
            result = new LazyTableShard(path, env);
          }
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
        final MRTableShard shd = shardAlter.shard;
        updateResource(shd);
        break;
    }
  }

  private void updateResource(MRTableShard shd) {
    final String path = shd.path();
    for (final String resourceName : keys()) {
      final Object resolve = get(resourceName);
      if (resolve instanceof MRTableShard) {
        final MRTableShard oldShard = (MRTableShard) resolve;
        if (path.equals(oldShard.path()))
          set(resourceName, shd);
      }
      else if (resolve instanceof MRTableShard[]) {
        final MRTableShard[] shards = (MRTableShard[]) resolve;
        for(int i = 0; i < shards.length; i++) {
          final MRTableShard oldShard = shards[i];
          if (path.equals(oldShard.path())) {
            shards[i] = shd;
          }
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void sync() {
    final long currentTime = System.currentTimeMillis();
    final List<String> shards = new ArrayList<>();
    for (final String resourceName : keys()) {
      processAs(resourceName, new Processor<MRTableShard>() {
        @Override
        public void process(final MRTableShard shard) {
          if (hints.contains(shard.path()) || currentTime - shard.metaTS() > MRTools.FRESHNESS_TIMEOUT) {
            shards.add(shard.path());
          }
        }
      });
    }
    // this will update all shards through notification mechanism
    env.resolveAll(shards.toArray(new String[shards.size()]));

    for (Map.Entry<Object, Object> entry : increment.entrySet()) {
      if (CharSeq.EMPTY == entry.getValue())
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
          if (!processAs(resourceName, new Processor<MRTableShard>() {
            @Override
            public void process(MRTableShard arg) {
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

  public static class LazyTableShard extends MRTableShard {
    private final MREnv env;
    MRTableShard realShard;

    public LazyTableShard(String path, MREnv env) {
      super(path, false, false, "", 0, 0, 0, 0);
      this.env = env;
    }

    private MRTableShard real() {
      if (realShard == null)
        realShard = env.resolve(path());
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
      return 0l;
    }

    @Override
    public long recordsCount() {
      return real().recordsCount();
    }

    @Override
    public long length() {
      return real().length();
    }

    @Override
    public long keysCount() {
      return real().keysCount();
    }
  }
}
