package ru.yandex.se.yasm4u.domains.mr.env;

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
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.mr.*;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.impl.DefaultMRErrorsHandler;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRTableState;

import java.io.*;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by minamoto on 10/12/14.
 */
public abstract class RemoteMREnv extends MREnvBase {
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
    if (env instanceof LocalMREnv)
      env.execute(builder, new DefaultMRErrorsHandler()); // to be able to debug
    try {
      final File jar = File.createTempFile("yamr-routine-", ".jar");
      //noinspection ResultOfMethodCallIgnored
      jar.delete();
      jar.deleteOnExit();
      process = runJvm(MRRunner.class, "--dump", jar.getAbsolutePath());
      final ByteArrayOutputStream builderSerialized = new ByteArrayOutputStream();
      try (final ObjectOutputStream outputStream = new ObjectOutputStream(builderSerialized)) {
        outputStream.writeObject(builder);
      }
      final Writer to = new OutputStreamWriter(process.getOutputStream(), StreamTools.UTF);
      final Reader from = new InputStreamReader(process.getInputStream(), StreamTools.UTF);
      to.append(CharSeqTools.toBase64(builderSerialized.toByteArray())).append("\n");
      to.flush();
      final MROutputBase output = new MROutput2MREnv(env, builder.output(), null);
      final Thread asyncReaderTh = new Thread() {
        @Override
        public void run() {
          try {
            CharSeqTools.processLines(from, new Processor<CharSequence>() {
              @Override
              public void process(CharSequence arg) {
                output.parse(arg);
              }
            });
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      };
      asyncReaderTh.setDaemon(true);
      asyncReaderTh.start();
      final MRPath[] input = builder.input();
      final Set<String> visited = new HashSet<>();
      MRPath parent = null;
      for (int i = 0; i < input.length; i++) {
        final String realPath = env instanceof LocalMREnv ? ((LocalMREnv) env).file(input[i]).getAbsolutePath() : input[i].toString();
        if (visited.contains(realPath))
          continue;
        to.append(Integer.toString(i)).append("\n");
        to.flush();
        if (input[i].mount == MRPath.Mount.LOG_BROKER ) {
          if (parent != null && parent == input[i].parent()) {
            visited.add(realPath);
            continue;
          } else {
            parent = input[i].parent();
          }
        }
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
        visited.add(realPath);
      }
      to.close();

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

  protected final Map<MRPath, MRTableState> shardsCache = new HashMap<>();
//  protected final FixedSizeCache<MRPath, MRTableState> shardsCache = new FixedSizeCache<>(10000, CacheStrategy.Type.LRU);

  protected synchronized void updateState(MRPath path, MRTableState newState) {
    shardsCache.put(path, newState);
    if (path.sorted) {
      shardsCache.put(path.mkunsorted(), newState);
    }
    else {
      shardsCache.remove(path.mksorted());
    }
  }

  protected synchronized void wipeState(MRPath path) {
    shardsCache.remove(path);
    shardsCache.remove(path.sorted ? path.mkunsorted() : path.mksorted());
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
    return resolveAll(paths, false, MRTools.DIR_FRESHNESS_TIMEOUT);
  }

  protected MRTableState resolve(MRPath path, boolean cachedOnly) {
    return resolveAll(new MRPath[]{path}, cachedOnly, MRTools.DIR_FRESHNESS_TIMEOUT)[0];
  }

  protected final MRTableState[] resolveAll(MRPath[] paths, boolean cachedOnly, long freshnesstimeout) {
    final MRTableState[] result = new MRTableState[paths.length];
    final long time = System.currentTimeMillis();
    final Set<MRPath> unknown = new HashSet<>();
    for(int i = 0; i < paths.length; i++) {
      MRPath path = paths[i];
      if (path.isDirectory())
        throw new IllegalArgumentException("Path must not be directory");

      final MRTableState shard = cachedState(path);
      if (shard != null) {
        if (time - shard.snapshotTime() < freshnesstimeout) {
          result[i] = shard;
          continue;
        }
      }
      unknown.add(path);
    }

    if (cachedOnly)
      return result;

    if (Boolean.getBoolean("yasm4u.getInsteadOfList")) {
      MRPath parent = null;
      for (final MRPath u : unknown) {
        if (u.isDirectory()) {
          if (u.isMountRoot())
            continue;
          list(u);
        }
        else {
          if (u.mount == MRPath.Mount.LOG_BROKER) {
            if (parent == null || !parent.equals(u.parent())) {
              parent = u.parent();
              list(parent);
            }
          }
          else
            get(u);
        }
      }
    }
    else {
      // after this cycle all entries of paths array must be in the shardsCache
      Set<MRPath> pathSet = findBestPrefixes(unknown);
      for (final MRPath prefix : pathSet) {
        final MRPath toList = prefix.isDirectory() ? prefix : prefix.parent();
        list(toList);
      }
    }
    final MRTableState[] states = resolveAll(paths, true, freshnesstimeout + TimeUnit.SECONDS.toMillis(30));
    for(int i = 0; i < states.length; i++) {
      if (states[i] != null)
        continue;
      states[i] = new MRTableState(localPath(paths[i]), paths[i].sorted);
      updateState(paths[i], states[i]);
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
      if (parent.isMountRoot() || isFat(path)) {
        result.add(path);
        itPath.remove();
      }
      else parents.adjustOrPutValue(parent, 1, 1);
    }

    for(final MRPath path : paths) {
      final MRPath parent = path.parent();

      if (isFat(parent)) {
        result.add(path);
        parents.remove(parent);
      }
    }
    final Set<MRPath> bestPrefixes = findBestPrefixes(parents.keySet());
    for(final MRPath path : paths) {
      final MRPath parent = path.parent();

      if (bestPrefixes.contains(parent) && parents.get(parent) == 1) {
        result.add(path);
        bestPrefixes.remove(parent);
      }
    }

    result.addAll(bestPrefixes);
    return result;
  }

  /* copy from commons */
  private static Process runJvm(final Class<?> mainClass, final String... args) {
    try {
      final Method main = mainClass.getMethod("main", String[].class);
      if (main.getReturnType().equals(void.class)
          && Modifier.isStatic(main.getModifiers())
          && Modifier.isPublic(main.getModifiers())) {
        try {
          final List<String> parameters = new ArrayList<>();
          parameters.add(System.getProperty("java.home") + "/bin/java");
          parameters.add("-Xmx3g");
          parameters.add("-classpath");
          parameters.add(System.getProperty("yasm4u.class.path", System.getProperty("java.class.path")));
          parameters.add(mainClass.getName());
          parameters.addAll(Arrays.asList(args));
          System.err.println("runjvm: " + parameters.toString());
          return Runtime.getRuntime().exec(parameters.toArray(new String[parameters.size()]));
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    catch (NoSuchMethodException e) {
      //
    }
    throw new IllegalArgumentException("Main class must contain main method :)");
  }
}
