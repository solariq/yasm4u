package ru.yandex.se.yasm4u.domains.mr.env;

import com.spbsu.commons.func.Computable;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.io.QueueReader;
import com.spbsu.commons.seq.*;
import com.spbsu.commons.system.RuntimeUtils;
import com.spbsu.commons.util.ArrayTools;
import com.spbsu.commons.util.Holder;
import com.spbsu.commons.util.MultiMap;
import com.spbsu.commons.util.Pair;
import gnu.trove.list.array.TIntArrayList;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.domains.mr.*;
import ru.yandex.se.yasm4u.domains.mr.io.MRTableShardConverter;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRTableState;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

import java.io.File;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * User: solar
 * Date: 17.12.14
 * Time: 16:12
 */
public class CompositeMREnv extends MREnvBase {
  public static final MRPath SHARD = MRPath.create("/MREnvState");
  private final RemoteMREnv original;
  private final LocalMREnv localCopy;
  private final Map<MRPath, Pair<MRTableState, MRTableState>> tables = new HashMap<>();
  private final Map<MRPath, Pair<Long, MRPath[]>> dirs = new HashMap<>();

  public CompositeMREnv(RemoteMREnv original, LocalMREnv localCopy) {
    this.original = original;
    this.localCopy = localCopy;
    localCopy.read(SHARD, new Processor<MRRecord>() {
      @Override
      public void process(MRRecord arg) {
        final MRPath path = MRPath.createFromURI(arg.key);
        if (path.isDirectory()) {
          final CharSequence[] parts = CharSeqTools.split(arg.value, "\t");
          final long ts = CharSeqTools.parseLong(parts[0]);
          final List<MRPath> list = new ArrayList<>();
          for(int i = 1; i < parts.length; i++) {
            list.add(MRPath.createFromURI(parts[i].toString()));
          }
          dirs.put(path, Pair.create(ts, list.toArray(new MRPath[list.size()])));
        }
        else {
          final CharSequence[] parts = CharSeqTools.split(arg.value, "\t");
          final MRTableShardConverter.From converter = new MRTableShardConverter.From();
          tables.put(path, Pair.create(converter.convert(parts[0]), converter.convert(parts[1])));
        }
      }
    });
    for (Map.Entry<MRPath, Pair<MRTableState, MRTableState>> entry : tables.entrySet()) {
      original.updateState(entry.getKey(), entry.getValue().first);
    }
  }

  public CompositeMREnv(RemoteMREnv original) {
    this(original, new LocalMREnv(LocalMREnv.DEFAULT_HOME + "/" + RuntimeUtils.bashEscape(original.name())));
  }

  @Override
  public boolean execute(MRRoutineBuilder builder, final MRErrorsHandler errorsHandler) {
    final MRPath[] in = builder.input();
    final MRPath[] out = builder.output();
    final MRTableState[] localInBefore = localCopy.resolveAll(in);
    final MRTableState[] localInAfter = sync(in);
    final MRTableState[] localOutBefore = localCopy.resolveAll(out);

    final File jar = original.executeLocallyAndBuildJar(builder, localCopy);

    final MRTableState[] localOutAfter = localCopy.resolveAll(out);

    if (Arrays.equals(localInAfter, localInBefore) && Arrays.equals(localOutAfter, localOutBefore)) {
      boolean outputExistence = true;
      for(int i = 0; i < out.length; i++) {
        outputExistence &= localOutAfter[i].isAvailable();
      }
      if (Boolean.getBoolean("yasm4u.dumpStatesFF")) {
        for(final MRTableState s:localInBefore)
          System.out.println("FF: local IN BEFORE: " + s);
        for(final MRTableState s:localOutBefore)
          System.out.println("FF: local OUT BEFORE: " + s);
        for(final MRTableState s:localInAfter)
          System.out.println("FF: local IN AFTER: " + s);
        for(final MRTableState s:localOutAfter)
          System.out.println("FF: local OUT AFTER: " + s);
      }
      if (outputExistence) {
        boolean ffDisabled = Boolean.getBoolean("yasm4u.disableFF");
        System.out.println("Fast forwarding execution " + (ffDisabled? "(ignored)" : "") + builder.toString());
        if (!ffDisabled) {
          final MRTableState[] outAfter = original.resolveAll(out, false);
          for(int i = 0; i < out.length; i++) {
            setCopy(out[i], outAfter[i], localOutAfter[i]);
          }

          return true;
        }
      }
    }
    final MultiMap<MRPath, CharSequence> needToAddToSample = new MultiMap<>();
    try {
      final boolean rc = original.execute(builder, new MRErrorsHandler() {
        int counter = 0;

        @Override
        public void error(String type, String cause, MRRecord record) {
          needToAddToSample.put(record.source, record.toString());
          errorsHandler.error(type, cause, record);
          counter++;
        }

        @Override
        public void error(Throwable th, MRRecord record) {
          needToAddToSample.put(record.source, record.toString());
          errorsHandler.error(th, record);
          counter++;
        }

        @Override
        public int errorsCount() {
          return counter;
        }
      }, jar);
      if (!rc) {
        for(int i = 0; i < out.length; i++) {
          setCopy(out[i], null, null);
        }
        return false;
      }
      final MRTableState[] outAfter = original.resolveAll(out, false);
      for(int i = 0; i < out.length; i++) {
        setCopy(out[i], outAfter[i], localOutAfter[i]);
      }
    }
    finally {
      for (final MRPath path : needToAddToSample.getKeys()) {
        localCopy.append(path, new CharSeqReader(CharSeqTools.concatWithDelimeter("\n", new ArrayList<>(needToAddToSample.get(path)))));
      }
    }

    return true;
  }

  public MRTableState[] sync(final MRPath... paths) {
    final MRTableState[] result = new MRTableState[paths.length];
    final MRTableState[] originalStates = resolveAll(paths);
    for(int i = 0; i < paths.length; i++) {
      final MRPath path = paths[i];
      if (!originalStates[i].isAvailable()) {
        localCopy.delete(path);
        tables.remove(path);
        continue;
      }
      final MRTableState local = localShard(path, originalStates[i]);
      if (local != null && local.isAvailable()) {
        result[i] = local;
        continue;
      }

      localShard(path, originalStates[i]);
      final ArrayBlockingQueue<CharSeq> readqueue = new ArrayBlockingQueue<>(1000);
      final Holder<MRTableState> localShardHolder = new Holder<>();
      final Thread readThread = new Thread() {
        @Override
        public void run() {
          localCopy.write(path, new QueueReader(readqueue));
          localShardHolder.setValue(localCopy.resolve(path));
        }
      };
      readThread.setDaemon(true);
      readThread.start();
      original.sample(path, new Processor<MRRecord>() {
        @Override
        public void process(MRRecord arg) {
          try {
            readqueue.put(new CharSeqComposite(arg.toString(), new CharSeqChar('\n')));
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      });
      try {
        readqueue.put(CharSeq.EMPTY);
        readThread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      setCopy(path, originalStates[i], result[i] = localShardHolder.getValue());
    }
    return result;
  }

  @Override
  public void get(MRPath prefix) {
    if (prefix.isDirectory())
      throw new IllegalArgumentException("Prefix must be table");
    list(prefix.parent());
  }

  @Override
  public MRPath[] list(MRPath prefix) {
    final Pair<Long, MRPath[]> cache = dirs.get(prefix);
    final long now = System.currentTimeMillis();
    if (cache != null && now - cache.first < MRTools.FRESHNESS_TIMEOUT) {
      final MRPath[] repack = ArrayTools.map(cache.second, MRPath.class, new Computable<Ref, MRPath>() {
        @Override
        public MRPath compute(Ref argument) {
          return (MRPath) argument;
        }
      });
      resolveAll(repack);
      return repack;
    }
    final MRPath[] result = original.list(prefix);
    dirs.put(prefix, Pair.create(now, result));
    write();
    return result;
  }

  @Override
  public MRTableState resolve(MRPath path) {
    return resolveAll(path)[0];
  }

  @Override
  public MRTableState[] resolveAll(MRPath... path) {
    final long now = System.currentTimeMillis();
    final MRTableState[] result = new MRTableState[path.length];
    final List<MRPath> toResolve = new ArrayList<>();
    final TIntArrayList toResolvePositions = new TIntArrayList();
    for(int i = 0; i < path.length; i++) {
      final MRPath mrPath = path[i];
      if (mrPath.isDirectory())
        throw new IllegalArgumentException("Resolved resource must not be directory: " + mrPath);
      final Pair<MRTableState, MRTableState> cache = tables.get(mrPath);
      if (cache == null || now - cache.first.snapshotTime() >= MRTools.FRESHNESS_TIMEOUT) {
        toResolve.add(mrPath);
        toResolvePositions.add(i);
      } else {
        result[i] = cache.second;
      }
    }
    final MRTableState[] resolvedAtOriginal = original.resolveAll(toResolve.toArray(new MRPath[toResolve.size()]));
    for(int i = 0; i < resolvedAtOriginal.length; i++) {
      final int originalIndex = toResolvePositions.get(i);
      result[originalIndex] = resolvedAtOriginal[i];
      localShard(path[originalIndex], resolvedAtOriginal[i]);
    }
    return result;
  }

  @Override
  public int read(MRPath shard, Processor<MRRecord> seq) {
    return original.read(shard, seq);
  }

  @Override
  public void sample(MRPath shard, Processor<MRRecord> seq) {
    sync(shard);
    localCopy.read(shard, seq);
  }

  @Override
  public void write(MRPath shard, Reader content) {
    original.write(shard, content);
    localCopy.write(shard, content);
    setCopy(shard, original.resolve(shard), localCopy.resolve(shard));
  }

  @Override
  public void append(MRPath shard, Reader content) {
    if (localShard(shard, null) != null) {
      original.append(shard, content);
      localCopy.append(shard, content);
      setCopy(shard, original.resolve(shard), localCopy.resolve(shard));
    }
    else {
      original.append(shard, content);
      setCopy(shard, null, null);
    }
  }

  @Override
  public void copy(MRPath[] from, MRPath to, boolean append) {
    original.copy(from, to, append);
    setCopy(to, null, null);
  }

  @Override
  public void delete(MRPath shard) {
    localCopy.delete(shard);
    original.delete(shard);
    tables.remove(shard);
    setCopy(shard, null, null);
  }

  private MRTableState localShard(MRPath shard, MRTableState state) {
    final Pair<MRTableState, MRTableState> localState = tables.get(shard);
    if (localState == null)
      return null;
    final MRTableState original = localState.getFirst();
    final MRTableState newCopy = localCopy.resolve(shard);
    if ((state != null && !original.equals(state)) || !newCopy.isAvailable()) {
      setCopy(shard, original, null);
      return null;
    }
    else if (state != null && state.snapshotTime() > original.snapshotTime()) {
      setCopy(shard, state, newCopy); // update metaTS
    }

    return newCopy;
  }

  private void setCopy(MRPath path, MRTableState original, MRTableState local) {
    if (local != null) {
      tables.put(path, Pair.create(original, local));
    }
    else tables.remove(path);
    write();
  }

  private void write() {
    final CharSeqBuilder builder = new CharSeqBuilder();
    final MRTableShardConverter.To converter = new MRTableShardConverter.To();

    for (Map.Entry<MRPath, Pair<MRTableState, MRTableState>> entry : tables.entrySet()) {
      builder.append(entry.getKey().toString()).append("\t");
      builder.append("\t");
      builder.append(converter.convert(entry.getValue().first)).append("\t");
      builder.append(converter.convert(entry.getValue().second)).append("\n");
    }

    for (Map.Entry<MRPath, Pair<Long, MRPath[]>> entry : dirs.entrySet()) {
      builder.append(entry.getKey().toString()).append("\t").append("\t");
      builder.append(entry.getValue().first);
      for (MRPath path : entry.getValue().second) {
        builder.append("\t").append(path);
      }
      builder.append("\n");
    }
    localCopy.write(SHARD, new CharSeqReader(builder.build()));
  }

  @Override
  public void sort(MRPath shard) {
    final MRTableState unsorted = original.resolve(shard, true);
    final MRTableState localShard = localShard(shard, unsorted);
    original.sort(shard);
    if (localShard != null) {
      final MRPath sortedPath = new MRPath(shard.mount, shard.path, true);
      final MRTableState sortedOriginal = original.resolve(sortedPath);
      localCopy.sort(shard);
      setCopy(sortedPath, sortedOriginal, localCopy.resolve(sortedPath));
    }
  }

  @Override
  public String name() {
    return original.name();
  }

  @Override
  public String toString() {
    return "CompositeMREnv(" + original.toString() + "," + localCopy.toString() + ")";
  }
}
