package solar.mr.env;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.system.RuntimeUtils;
import com.spbsu.commons.util.cache.CacheStrategy;
import com.spbsu.commons.util.cache.impl.FixedSizeCache;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.log4j.Logger;
import solar.mr.*;
import solar.mr.proc.impl.MRPath;
import solar.mr.routines.MRRecord;

import java.io.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by minamoto on 10/12/14.
 */
public abstract class RemoteMREnv implements MREnv {
  private static Logger LOG = Logger.getLogger(RemoteMREnv.class);
  protected final String user;

  protected final String master;
  protected final Action<CharSequence> defaultErrorsProcessor;
  protected final Action<CharSequence> defaultOutputProcessor;
  protected final ProcessRunner runner;

  protected RemoteMREnv(final ProcessRunner runner, final String user, final String master) {
    this(runner, user, master,
        new Action<CharSequence>() {
          @Override
          public void invoke(final CharSequence arg) {
            System.err.println(arg);
          }
        },
        new Action<CharSequence>() {
          @Override
          public void invoke(final CharSequence arg) {
            System.out.println(arg);
          }
        }
    );
  }

  protected RemoteMREnv(final ProcessRunner runner, final String user, final String master,
                        final Action<CharSequence> errorsProc,
                        final Action<CharSequence> outputProc) {
    this.runner = runner;
    this.user = user;
    this.master = master;
    this.defaultErrorsProcessor = errorsProc;
    this.defaultOutputProcessor = outputProc;
  }

  @Override
  public final boolean execute(MRRoutineBuilder exec, MRErrorsHandler errorsHandler) {
    final File jar = executeLocallyAndBuildJar(exec, this);
    return execute(exec, errorsHandler, jar);
  }

  public abstract boolean execute(MRRoutineBuilder exec, MRErrorsHandler errorsHandler, File jar);

  public File executeLocallyAndBuildJar(MRRoutineBuilder builder, MREnv env) {
    Process process = null;
    try {
      final File jar = File.createTempFile("yamr-routine-", ".jar");
      //noinspection ResultOfMethodCallIgnored
      jar.delete();
      jar.deleteOnExit();
      process = RuntimeUtils.runJvm(MRRunner.class, "--dump", jar.getAbsolutePath());
      final ByteArrayOutputStream builderSerialized = new ByteArrayOutputStream();
      try (final ObjectOutputStream outputStream = new ObjectOutputStream(builderSerialized)) {
        outputStream.writeObject(builder);
      }
      final Writer to = new OutputStreamWriter(process.getOutputStream(), StreamTools.UTF);
      final Reader from = new InputStreamReader(process.getInputStream(), StreamTools.UTF);
      to.append(CharSeqTools.toBase64(builderSerialized.toByteArray())).append("\n");
      to.flush();

      final MRPath[] input = builder.input();
      for (int i = 0; i < input.length; i++) {
        env.sample(input[i], new Processor<MRRecord>() {
          @Override
          public void process(MRRecord arg) {
            try {
              to.append(arg.toString()).append("\n");
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
      }
      to.close();
      final MROutputBase output = new MROutput2MREnv(env, builder.output(), null);
      CharSeqTools.processLines(from, new Processor<CharSequence>() {
        @Override
        public void process(CharSequence arg) {
          output.parse(arg);
        }
      });

      process.waitFor();
      output.interrupt();
      output.join();
      return jar;
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    finally {
        try {
          if (process != null) {
            process.getOutputStream().close();
            process.getInputStream().close();
            process.waitFor();
            StreamTools.transferData(process.getErrorStream(), System.err);
          }
        } catch (IOException | InterruptedException e) {
          LOG.warn(e);
        }
    }
  }

  protected void executeCommand(final List<String> options, final Action<CharSequence> outputProcessor,
                              final Action<CharSequence> errorsProcessor, InputStream contents) {
    try {
      final Process exec = runner.start(options, contents);
      if (exec == null)
        return;
      final Thread outThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            CharSeqTools.processLines(new InputStreamReader(exec.getInputStream(), StreamTools.UTF), outputProcessor);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      final Thread errThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            CharSeqTools.processLines(new InputStreamReader(exec.getErrorStream(), StreamTools.UTF), errorsProcessor);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      if (exec.getOutputStream() != null)
        exec.getOutputStream().close();
      outThread.start();
      errThread.start();
      exec.waitFor();
      outThread.join();
      errThread.join();
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  protected abstract MRPath findByLocalPath(String table, boolean sorted);
  @SuppressWarnings("UnusedDeclaration")
  protected abstract String localPath(MRPath shard);

  protected final FixedSizeCache<MRPath, MRTableState> shardsCache = new FixedSizeCache<>(10000, CacheStrategy.Type.LRU);

  protected synchronized void updateState(MRPath path, MRTableState newState) {
    shardsCache.put(path, newState);
  }

  protected synchronized void wipeState(MRPath path) {
    shardsCache.clear(path);
  }

  protected synchronized MRTableState cachedState(MRPath path) {
    MRTableState result = shardsCache.get(path);
    if (result == null && !path.sorted) {
      result = shardsCache.get(new MRPath(path.mount, path.path, true));
    }
    return result;
  }

  @Override
  public final MRTableState resolve(final MRPath path) {
    return resolve(path, false);
  }

  @Override
  public final MRTableState[] resolveAll(MRPath[] paths) {
    return resolveAll(paths, false);
  }

  protected MRTableState resolve(MRPath path, boolean cachedOnly) {
    return resolveAll(new MRPath[]{path}, cachedOnly)[0];
  }

  protected final MRTableState[] resolveAll(MRPath[] paths, boolean cachedOnly) {
    final MRTableState[] result = new MRTableState[paths.length];
    final long time = System.currentTimeMillis();
    final Set<MRPath> unknown = new HashSet<>();
    for(int i = 0; i < paths.length; i++) {
      MRPath path = paths[i];
      final MRTableState shard = cachedState(path);
      if (shard != null) {
        if (time - shard.snapshotTime() < MRTools.FRESHNESS_TIMEOUT) {
          result[i] = shard;
          continue;
        }
        wipeState(path);
      }
      unknown.add(path);
    }

    if (cachedOnly)
      return result;
    // after this cycle all entries of paths array must be in the shardsCache
    for (final MRPath prefix : findBestPrefixes(unknown)) {
      list(prefix);
    }
    final MRTableState[] states = resolveAll(paths, true);
    for(int i = 0; i < states.length; i++) {
      if (states[i] != null)
        continue;
      states[i] = new MRTableState(paths[i].path, false, paths[i].sorted, "", 0, 0, 0, System.currentTimeMillis());
    }
    return states;
  }

  protected abstract boolean isFat(MRPath path);

  private Set<MRPath> findBestPrefixes(Set<MRPath> paths) {
    if (paths.size() < 2)
      return paths;
    final Set<MRPath> result = new HashSet<>();
    final TObjectIntMap<MRPath> parents = new TObjectIntHashMap<>();
    final Iterator<MRPath> itPath = paths.iterator();
    while (itPath.hasNext()) {
      final MRPath path = itPath.next();
      final MRPath parent = path.parent();
      if (parent.isRoot() && isFat(path)) {
        result.add(path);
        itPath.remove();
      }
      else parents.adjustOrPutValue(parent, 1, 1);
    }

    for(final MRPath path : paths) {
      final MRPath parent = path.parent();

      if (parents.get(parent) == 1 && (parent.level() < 2 || parent.mount == MRPath.Mount.TEMP)) {
        result.add(path);
        parents.remove(parent);
      }
    }
    result.addAll(findBestPrefixes(parents.keySet()));
    return result;
  }

}
