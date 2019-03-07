package com.expleague.yasm4u.domains.mr.env;

import com.expleague.commons.io.QueueReader;
import com.expleague.commons.seq.*;
import com.expleague.commons.system.RuntimeUtils;
import com.expleague.commons.util.ArrayTools;
import com.expleague.commons.util.Holder;
import com.expleague.commons.util.MultiMap;
import com.expleague.commons.util.Pair;
import com.expleague.yasm4u.domains.mr.MRErrorsHandler;
import com.expleague.yasm4u.domains.mr.MRTools;
import com.expleague.yasm4u.domains.mr.ops.impl.MRTableState;
import gnu.trove.list.array.TIntArrayList;
import org.jetbrains.annotations.Nullable;
import com.expleague.yasm4u.domains.mr.io.MRTableShardConverter;
import com.expleague.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

import java.io.File;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
    localCopy.read(SHARD, arg -> {
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
        setCache(path, converter.convert(parts[0]), converter.convert(parts[1]));
      }
    });
    for (MRPath entry : cached()) {
      original.updateState(entry, originalShard(entry));
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

    final boolean ffTurnedOff = Boolean.getBoolean("yasm4u.disableFF");
    if (!ffTurnedOff && Arrays.equals(localInAfter, localInBefore) && Arrays.equals(localOutAfter, localOutBefore)) {
      final MRTableState[] outBefore = original.resolveAll(out, false, TimeUnit.MINUTES.toMillis(1));
      boolean outputExistence = true;
      for (int i = 0; i < out.length; i++) {
        outputExistence &= localOutAfter[i].isAvailable() && outBefore[i].isAvailable();
      }
      if (Boolean.getBoolean("yasm4u.dumpStatesFF")) {
        for (final MRTableState s : localInBefore)
          System.out.println("FF: local IN BEFORE: " + s);
        for (final MRTableState s : localOutBefore)
          System.out.println("FF: local OUT BEFORE: " + s);
        for (final MRTableState s : localInAfter)
          System.out.println("FF: local IN AFTER: " + s);
        for (final MRTableState s : localOutAfter)
          System.out.println("FF: local OUT AFTER: " + s);
      }
      if (outputExistence) {
        System.out.println("Fast forwarding execution " + builder.toString());
        for (int i = 0; i < out.length; i++) {
          setCopy(out[i], outBefore[i], localOutAfter[i]);
        }

        return true;
      }
    }
    final MultiMap<MRPath, CharSequence> needToAddToSample = new MultiMap<>();
    final List<Throwable> exceptions = new ArrayList<>();
    final List<Pair<String,String>> errors = new ArrayList<>();
    final List<MRRecord> records = new ArrayList<>();
    final TIntArrayList recordProblems = new TIntArrayList();
    try {
      final boolean rc = original.execute(builder, new MRErrorsHandler() {
        int counter = 0;

        @Override
        public void error(String type, String cause, MRRecord record) {
          needToAddToSample.put(record.source, record.toCharSequence());
          records.add(record);
          recordProblems.add(1);
          errors.add(Pair.create(type, cause));
          counter++;
        }

        @Override
        public void error(Throwable th, MRRecord record) {
          needToAddToSample.put(record.source, record.toCharSequence());
          records.add(record);
          recordProblems.add(0);
          exceptions.add(th);
          counter++;
        }

        @Override
        public int errorsCount() {
          return counter;
        }
      }, jar);
      if (!rc) {
        for (final MRPath anOut : out) {
          setCopy(anOut, null, null);
          original.delete(anOut);
        }
        return false;
      }

      final MRTableState[] outAfter = original.resolveAll(out, false, 0);
      for(int i = 0; i < out.length; i++) {
        if (ffTurnedOff)
          setCopy(out[i], null, null);
        else
          setCopy(out[i], outAfter[i], localOutAfter[i]);
      }
    }
    finally {
      for (final MRPath path : needToAddToSample.getKeys()) {
        localCopy.append(path, new CharSeqReader(CharSeqTools.concatWithDelimeter("\n", new ArrayList<>(needToAddToSample.get(path)))));
      }
      int exIndex = 0;
      int errIndex = 0;
      for (int i = 0; i < records.size(); i++) {
        final MRRecord record = records.get(i);
        if (recordProblems.get(i) != 0) {
          final Pair<String, String> error = errors.get(errIndex++);
          errorsHandler.error(error.first, error.second, record);
        }
        else
          errorsHandler.error(exceptions.get(exIndex++), record);
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
        setCopy(path, null, null);
        continue;
      }
      final MRTableState local = localShard(path, originalStates[i]);
      if (local != null && local.isAvailable() && (local.recordsCount() > 0 || originalStates[i].recordsCount() == 0)) {
        result[i] = local;
        continue;
      }

//      localShard(path, originalStates[i]);
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
      original.sample(path, arg -> {
        try {
          readqueue.put(new CharSeqComposite(arg.toString(), new CharSeqChar('\n')));
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
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
  public void update(MRPath prefix) {
    if (prefix.isDirectory())
      throw new IllegalArgumentException("Prefix must be table");
    list(prefix.parent());
  }

  @Override
  public MRPath[] list(MRPath prefix) {
    final Pair<Long, MRPath[]> cache = dirs.get(prefix);
    final long now = System.currentTimeMillis();
    if (cache != null && now - cache.first < MRTools.DIR_FRESHNESS_TIMEOUT) {
      final MRPath[] repack = ArrayTools.map(cache.second, MRPath.class, argument -> argument);
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
    final MRTableState[] result = new MRTableState[path.length];
    final List<MRPath> toResolve = new ArrayList<>();
    final TIntArrayList toResolvePositions = new TIntArrayList();
    for(int i = 0; i < path.length; i++) {
      final MRPath mrPath = path[i];
      if (mrPath.isDirectory())
        throw new IllegalArgumentException("Resolved resource must not be directory: " + mrPath);
      final MRTableState originalShard = originalShard(mrPath);
      if (originalShard == null) {
        toResolve.add(mrPath);
        toResolvePositions.add(i);
      } else {
        result[i] = originalShard;
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
  public int read(MRPath shard, Consumer<MRRecord> seq) {
    return original.read(shard, seq);
  }

  @Override
  public void sample(MRPath shard, Consumer<MRRecord> seq) {
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
    setCopy(shard, null, null);
  }

  @Nullable
  private synchronized MRTableState localShard(MRPath shard, MRTableState state) {
    if ((shard.mount == MRPath.Mount.LOG
    || shard.mount == MRPath.Mount.LOG_BROKER) && !Boolean.getBoolean("yasm4u.test")) { // logs must not change their structure over time. No additional checks necessarily
      final MRTableState resolve = localCopy.resolve(shard);
      if (state != null)
        tables.put(shard, Pair.create(state, resolve));
      return resolve;
    }

    final Pair<MRTableState, MRTableState> localState = tables.get(shard);
    if (localState == null)
      return null;
    final MRTableState original = localState.getFirst();
    final MRTableState newCopy = localCopy.resolve(shard);

    if (state != null) {
      if (!original.equals(state)) {
        setCopy(shard, null, null);
        return null;
      } else if (state.snapshotTime() > original.snapshotTime()) {
        setCopy(shard, state, newCopy); // update metaTS
      }
    }

    return newCopy.isAvailable() ? newCopy : null;
  }

  @Nullable
  private synchronized MRTableState originalShard(MRPath entry) {
    final Pair<MRTableState, MRTableState> localState = tables.get(entry);
    if (localState == null)
      return null;
    final MRTableState originalShard = localState.getFirst();
    if (System.currentTimeMillis() - originalShard.snapshotTime() >= MRTools.TABLE_FRESHNESS_TIMEOUT) {
      setCopy(entry, null, null);
      return null;
    }
    return originalShard;
  }

  private synchronized Collection<MRPath> cached() {
    return new ArrayList<>(tables.keySet());
  }

  private synchronized void setCache(MRPath path, MRTableState original, MRTableState local) {
    if (local != null)
      tables.put(path, Pair.create(original, local));
    else tables.remove(path);
  }

  private void setCopy(MRPath path, MRTableState original, MRTableState local) {
    setCache(path, original, local);
    if (local == null)
      localCopy.delete(path);
    write();
  }

  private synchronized void write() {
    final CharSeqBuilder builder = new CharSeqBuilder();
    final MRTableShardConverter.To converter = new MRTableShardConverter.To();

    for (MRPath entry : cached()) {
      final MRTableState local = localShard(entry, null);
      final MRTableState original = originalShard(entry);
      if (original == null || local == null) {
        originalShard(entry);
        localShard(entry, null);
        continue;
      }
      builder.append(entry.toString()).append("\t");
      builder.append("\t");
      builder.append(converter.convert(original)).append("\t");
      builder.append(converter.convert(local)).append("\n");
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
  public long key(MRPath shard, String key, Consumer<MRRecord> seq) {
    return original.key(shard, key, seq);
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
