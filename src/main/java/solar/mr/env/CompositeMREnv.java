package solar.mr.env;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqAdapter;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.system.RuntimeUtils;
import com.spbsu.commons.util.Holder;
import com.spbsu.commons.util.MultiMap;
import com.spbsu.commons.util.Pair;
import org.jetbrains.annotations.NotNull;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.MRRoutineBuilder;
import solar.mr.MRTableShard;
import solar.mr.proc.Whiteboard;
import solar.mr.proc.impl.WhiteboardImpl;
import solar.mr.routines.MRRecord;

import java.io.File;
import java.io.IOException;
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
    copyState = new WhiteboardImpl(localCopy, "MREnvState(" + RuntimeUtils.bashEscape(original.name()) + ")", System.getenv("USER"));
  }

  public CompositeMREnv(RemoteMREnv original) {
    this(original, new LocalMREnv(LocalMREnv.DEFAULT_HOME + "/" + RuntimeUtils.bashEscape(original.name())));
  }

  @Override
  public boolean execute(MRRoutineBuilder builder, final MRErrorsHandler errorsHandler) {
    final MRTableShard[] in = original.resolveAll(builder.input());
    final MRTableShard[] out = original.resolveAll(builder.output());
    final MRTableShard[] localOutBefore = reflect(out);
    final MRTableShard[] localInBefore = reflect(in);
    final MRTableShard[] localInAfter = sync(in);

    final File jar = original.executeLocallyAndBuildJar(builder, localCopy);

    final MRTableShard[] localOutAfter = reflect(out);

    if (Arrays.equals(localInAfter, localInBefore) && Arrays.equals(localOutAfter, localOutBefore)) {
      boolean outputExistence = true;
      for(int i = 0; i < out.length; i++) {
        outputExistence &= out[i].isAvailable();
      }
      if (outputExistence) {
        System.out.println("Fast forwarding execution " + builder.toString());
        return true;
      }
    }
    final MultiMap<String, CharSequence> needToAddToSample = new MultiMap<>();
    try {
      if (!original.execute(builder, new MRErrorsHandler() {
        @Override
        public void error(String type, String cause, MRRecord record) {
          needToAddToSample.put(record.source, record.toString());
          errorsHandler.error(type, cause, record);
        }

        @Override
        public void error(Throwable th, MRRecord record) {
          needToAddToSample.put(record.source, record.toString());
          errorsHandler.error(th, record);
        }
      }, jar))
        return false;
      final MRTableShard[] outAfter = original.resolveAll(paths(out));
      for(int i = 0; i < out.length; i++) {
        copyState.set("mr://" + out[i].path(), Pair.create(outAfter[i], localOutAfter[i]));
      }
    }
    finally {
      for (final String path : needToAddToSample.getKeys()) {
        localCopy.append(localCopy.resolve(path), new CharSeqReader(CharSeqTools.concatWithDelimeter("\n", new ArrayList<>(needToAddToSample.get(path)))));
      }
    }

    return true;
  }

  public MRTableShard[] reflect(final MRTableShard... shards) {
    final String[] paths = paths(shards);
    return localCopy.resolveAll(paths);
  }

  public MRTableShard[] sync(final MRTableShard... shards) {
    MRTableShard[] result = new MRTableShard[shards.length];
    for(int i = 0; i < shards.length; i++) {
      final MRTableShard shard = shards[i];
      final Pair<MRTableShard,MRTableShard> remote2local = copyState.snapshot().get("mr://" + shard.path());
      if (remote2local != null && shard.equals(remote2local.second)) {
        result[i] = remote2local.second;
        continue;
      }

      final ArrayBlockingQueue<CharSeq> readqueue = new ArrayBlockingQueue<>(1000);
      final Holder<MRTableShard> localShardHolder = new Holder<>();
      final Thread readThread = new Thread() {
        @Override
        public void run() {
          localShardHolder.setValue(localCopy.write(shard, new Reader() {
            CharSeq take;
            int offset = 0;
            boolean closed = false;

            @Override
            public int read(@NotNull char[] cbuf, int off, int len) throws IOException {
              if (closed)
                return -1;
              if (take == null) {
                try {
                  take = readqueue.take();
                  offset = 0;
                  if (take == CharSeq.EMPTY)
                    throw new InterruptedException();
                } catch (InterruptedException e) {
                  closed = true;
                  return -1;
                }
              }
              final int copied = Math.min(take.length() - offset, len);
              take.copyToArray(offset, cbuf, off, copied);
              offset += copied;
              if (offset >= take.length())
                take = null;
              return copied;
            }

            @Override
            public void close() throws IOException {
              closed = true;
            }
          }));
        }
      };
      readThread.setDaemon(true);
      readThread.start();
      if (shard.length() < 10 * 1024 * 1024) {
        original.read(shard, new Processor<CharSequence>() {
          @Override
          public void process(CharSequence arg) {
            try {
              readqueue.put(new CharSeqAdapter(arg));
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        });
      } else {
        original.sample(shard, new Processor<CharSequence>() {
          @Override
          public void process(CharSequence arg) {
            try {
              readqueue.put(new CharSeqAdapter(arg));
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        });
      }
      try {
        readqueue.put(CharSeq.EMPTY);
        readThread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      copyState.set("mr://" + shard.path(), Pair.create(shard, result[i] = localShardHolder.getValue()));
    }
    return result;
  }

  private String[] paths(MRTableShard[] shards) {
    final String[] paths = new String[shards.length];
    for(int i = 0; i < paths.length; i++) {
      paths[i] = shards[i].path();
    }
    return paths;
  }

  @Override
  public MRTableShard[] list(String prefix) {
    return original.list(prefix);
  }

  @Override
  public MRTableShard resolve(String path) {
    return original.resolve(path);
  }

  @Override
  public MRTableShard[] resolveAll(String... path) {
    return original.resolveAll(path);
  }

  @Override
  public int read(MRTableShard shard, Processor<CharSequence> seq) {
    return original.read(shard, seq);
  }

  @Override
  public void sample(MRTableShard shard, Processor<CharSequence> seq) {
    original.read(shard, seq);
  }

  @Override
  public MRTableShard write(MRTableShard shard, Reader content) {
    return original.write(shard, content);
  }

  @Override
  public MRTableShard append(MRTableShard shard, Reader content) {
    return original.append(shard, content);
  }

  @Override
  public MRTableShard copy(MRTableShard[] from, MRTableShard to, boolean append) {
    return original.copy(from, to, append);
  }

  @Override
  public MRTableShard delete(MRTableShard shard) {
    return original.delete(shard);
  }

  @Override
  public MRTableShard sort(MRTableShard shard) {
    return original.sort(shard);
  }

  @Override
  public String name() {
    return original.name();
  }

  @Override
  public String getTmp() {
    return original.getTmp();
  }

  @Override
  public void addListener(Action<? super ShardAlter> lst) {
    original.addListener(lst);
  }

  @Override
  public String toString() {
    return "CompositeMREnv(" + original.toString() + "," + localCopy.toString() + ")";
  }
}
