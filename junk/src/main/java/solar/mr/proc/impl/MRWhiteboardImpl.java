package solar.mr.proc.impl;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;


import com.spbsu.commons.filters.*;
import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.types.ConversionRepository;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.system.RuntimeUtils;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.proc.MRState;
import solar.mr.proc.MRWhiteboard;
import solar.mr.tables.MRTableShard;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public class MRWhiteboardImpl implements MRWhiteboard {
  private final MREnv env;
  private final String user;
  private final MRStateImpl state = new MRStateImpl();
  private final MRTableShard myShard;
  private final Random rng = new FastRandom();
  private final MRErrorsHandler errorsHandler;
  private SerializationRepository<CharSequence> marshaling;

  public MRWhiteboardImpl(final MREnv env, final String id, final String user) {
    this(env, id, user, new MRErrorsHandler() {
      @Override
      public void error(final String type, final String cause, final String table, final CharSequence record) {
        System.err.println(table + "\t" + type + "\t" + cause);
        System.err.println(record);
      }

      @Override
      public void error(final Throwable th, final String table, final CharSequence record) {
        System.err.print(table + "\t");
        th.printStackTrace(System.err);
        System.err.println(record);
      }
    });
  }

  public MRWhiteboardImpl(final MREnv env, final String id, final String user, MRErrorsHandler handler) {
    marshaling = new SerializationRepository<>(MRState.SERIALIZATION).customize(
        new OrFilter<TypeConverter>(
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
    myShard = env.resolve("temp/" + user + "/state/" + id);
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
  }

  @Override
  public <T> T refresh(final String uri) {
    Object resource = resolve(uri);
    if (resource instanceof MRTableShard)
      set(uri, resource = ((MRTableShard) resource).refresh());
    return (T)resource;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T resolve(final String resource) {
    try {
      final String resolveResult = state.resolveVars(resource);
      if (resolveResult.indexOf(':') < 0)
        return (T)resolveResult;
      else if (state.state.containsKey(resolveResult))
        return state.get(resolveResult);

      final URI uri = new URI(resolveResult);
      final String scheme = uri.getScheme();
      switch(scheme) {
        case "temp":
          final String subProtocol = uri.getSchemeSpecificPart();
          if (subProtocol.startsWith("mr://")) {
            final MRTableShard resolve = env.resolve(
                "temp/" + user + subProtocol.substring("mr://".length()) + "-" + (Integer.toHexString(rng.nextInt())));
            set(resolveResult, resolve);
            return (T)resolve;
          }
          else throw new UnsupportedOperationException("Unknown schema for temp allocation: " + subProtocol);
        case "mr":
          final MRTableShard resolve = env.resolve(uri.getPath());
          set(resolveResult, resolve);
          return (T)resolve;
        case "var":
          return state.get(resolveResult);
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
  public SerializationRepository marshaling() {
    return marshaling;
  }

  @Override
  public <T> void set(final String uri, final T data) {
    if (data.equals(state.state.get(uri)))
      return;
    final Class<?> dataClass = data.getClass();
    final TypeConverter<T, CharSequence> converter = (TypeConverter<T, CharSequence>)marshaling.base.converter(dataClass, CharSequence.class);
    final Class[] typeParameters = RuntimeUtils.findTypeParameters(converter.getClass(), TypeConverter.class);
    env.append(myShard, new CharSeqReader(CharSeqTools.concat(CharSeqTools.concatWithDelimeter("\t", uri,
        typeParameters[0] != null ? typeParameters[0].getName() : dataClass.getName(), converter.convert(data)), "\n")));
    state.state.put(uri, data);
  }

  @Override
  public void remove(final String var) {
    state.state.remove(var);
    env.append(myShard, new CharSeqReader(var + "\t\n"));
  }

  @Override
  public MRState slice() {
    return new MRStateImpl(state);
  }

  @Override
  public boolean check(final String resource) {
    final Object resolve = resolve(resource);
    if (resolve instanceof MRTableShard) {
      final MRTableShard knownShard = (MRTableShard) resolve;
      final MRTableShard currentShard = env.resolve(knownShard.path());
      return knownShard.isAvailable() && currentShard.isAvailable() && currentShard.crc().equals(knownShard.crc());
    }
    return true;
  }

  @Override
  public boolean checkAll(final String[] productName) {
    for (String resource : productName) {
      if (!check(resource))
        return false;
    }
    return true;
  }

  @Override
  public void clear() {
    for (String resourceName : state.available()) {
      final Object resource = state.get(resourceName);
      if (resourceName.startsWith("temp:") && resource instanceof MRTableShard) {
        env.delete((MRTableShard)resource);
      }
    }
  }
}
