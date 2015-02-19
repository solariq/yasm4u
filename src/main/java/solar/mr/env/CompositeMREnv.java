package solar.mr.env;

import com.spbsu.commons.func.Processor;
import com.spbsu.commons.io.QueueReader;
import com.spbsu.commons.seq.*;
import com.spbsu.commons.system.RuntimeUtils;
import com.spbsu.commons.util.Holder;
import com.spbsu.commons.util.MultiMap;
import com.spbsu.commons.util.Pair;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.MRRoutineBuilder;
import solar.mr.MRTableState;
import solar.mr.proc.Whiteboard;
import solar.mr.proc.impl.MRPath;
import solar.mr.proc.impl.WhiteboardImpl;
import solar.mr.routines.MRRecord;

import java.io.File;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * User: solar
 * Date: 17.12.14
 * Time: 16:12
 */
public class CompositeMREnv implements MREnv {
  private final RemoteMREnv original;
  private final LocalMREnv localCopy;
  private final Whiteboard copyState;

  public CompositeMREnv(RemoteMREnv original, LocalMREnv localCopy) {
    this.original = original;
    this.localCopy = localCopy;
    copyState = new WhiteboardImpl(localCopy, "MREnvState(" + RuntimeUtils.bashEscape(original.name()) + ")", WhiteboardImpl.USER);
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
        if (!ffDisabled)
          return true;
      }
    }
    final MultiMap<MRPath, CharSequence> needToAddToSample = new MultiMap<>();
    try {
      if (!original.execute(builder, new MRErrorsHandler() {
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
      }, jar))
        return false;
      final MRTableState[] outAfter = original.resolveAll(out);
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
    final MRTableState[] originalStates = original.resolveAll(paths);
    for(int i = 0; i < paths.length; i++) {
      final MRPath path = paths[i];
      final MRTableState local = localShard(path, originalStates[i]);
      if (local != null && local.isAvailable()) {
        result[i] = local;
        continue;
      }

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
      if (originalStates[i] != null)
        setCopy(path, originalStates[i], result[i] = localShardHolder.getValue());
    }
    return result;
  }

  @Override
  public MRPath[] list(MRPath prefix) {
    return original.list(prefix);
  }

  @Override
  public MRTableState resolve(MRPath path) {
    return original.resolve(path);
  }

  @Override
  public MRTableState[] resolveAll(MRPath... path) {
    return original.resolveAll(path);
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
  }

  @Override
  public void append(MRPath shard, Reader content) {
    original.append(shard, content);
  }

  @Override
  public void copy(MRPath[] from, MRPath to, boolean append) {
    original.copy(from, to, append);
  }

  @Override
  public void delete(MRPath shard) {
    localCopy.delete(shard);
    original.delete(shard);
  }

  private MRTableState localShard(MRPath shard, MRTableState state) {
    final String key = shard.resource().toString();
    final Pair<MRTableState, MRTableState> localState = copyState.snapshot().get(key);
    if (localState == null)
      return null;
    final MRTableState original = localState.getFirst();
    final MRTableState local = localState.getSecond();
    if ((original.snapshotTime() < state.snapshotTime() && !original.equals(state)) ||
        !local.equals(localCopy.resolve(shard))) {
      setCopy(shard, original, null);
      return null;
    }

    return local;
  }

  private void setCopy(MRPath path, MRTableState original, MRTableState local) {
    final String uri = path.resource().toString();
    if (local != null) {
      copyState.set(uri, Pair.create(original, local));
      copyState.snapshot();
    }
    else copyState.remove(uri);
  }

  @Override
  public void sort(MRPath shard) {
    original.sort(shard);
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
