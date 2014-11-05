package solar.mr.proc.impl;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.MREnv;
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

  public MRWhiteboardImpl(final MREnv env, final String id, final String user) {
    this.env = env;
    this.user = user;
    myShard = env.resolve("temp/" + user + "/state/" + id);
    env.read(myShard, new Processor<CharSequence>() {
      private CharSequence[] buf = new CharSequence[3];

      @Override
      public void process(final CharSequence arg) {
        CharSequence[] parts = CharSeqTools.split(arg, '\t', buf);
        try {
          state.state.put(parts[0].toString(), MRState.SERIALIZATION.read(parts[2], Class.forName(parts[1].toString())));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T resolve(final String resource) {
    try {
      final String resolveResult = state.resolveVars(resource);
      if (resolveResult.indexOf(':') < 0)
        return (T)resolveResult;
      else if (state.available(resolveResult))
        return state.get(resolveResult);

      final URI uri = new URI(resolveResult);
      final String scheme = uri.getScheme();
      switch(scheme) {
        case "temp":
          final String subProtocol = uri.getSchemeSpecificPart();
          if ("mr".equals(subProtocol)) {
            final MRTableShard resolve = env.resolve(
                "temp/" + user + "/" + uri.getPath() + "-" + (Integer.toHexString(rng.nextInt())));
            set(resolveResult, resolve);
            return (T)resolve;
          }
          else throw new UnsupportedOperationException("Unknown schema for temp allocation: " + subProtocol);
        case "mr":
          final MRTableShard resolve = env.resolve(uri.getPath());
          set(resolveResult, resolve);
          return (T)resolve;
        case "var":
          return state.get(uri.getSchemeSpecificPart());
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
  public <T> void set(final String uri, final T data) {
    if (state.state.containsKey(uri))
      throw new IllegalArgumentException("Resource named " + uri + " already set to " + state.get(uri));
    if (MRState.SERIALIZATION.base.converter(data.getClass(), CharSequence.class) == null)
      throw new IllegalArgumentException("Can not find conversion in repository for specified type: " + uri + " already set to " + data.getClass().getName());
    env.append(myShard, new CharSeqReader(CharSeqTools.concatWithDelimeter("\t", uri, data.getClass().getName(), MRState.SERIALIZATION.write(data))));
    state.state.put(uri, data);
  }

  @Override
  public void remove(final String var) {
    state.state.remove(var);
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
      final MRTableShard[] shards = env.shards(knownShard.owner());
      if (shards.length > 1 || shards.length == 0)
        return false;
      return knownShard.crc().equals(shards[0].crc());
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
