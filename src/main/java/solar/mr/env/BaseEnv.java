package solar.mr.env;

import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.impl.WeakListenerHolderImpl;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.cache.CacheStrategy;
import com.spbsu.commons.util.cache.impl.FixedSizeCache;
import solar.mr.MREnv;
import solar.mr.MRTableShard;
import solar.mr.ProfilableMREnv;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by minamoto on 10/12/14.
 */
public abstract class BaseEnv extends WeakListenerHolderImpl<MREnv.ShardAlter> {
  protected final String user;

  protected final String master;
  protected final Processor<CharSequence> defaultErrorsProcessor;
  protected final Processor<CharSequence> defaultOutputProcessor;
  protected final ProcessRunner runner;
  protected final ClosureJarBuilder jarBuilder;

  protected BaseEnv(final ProcessRunner runner, final String user, final String master) {
    this(runner, user, master,
        new Processor<CharSequence>() {
          @Override
          public void process(final CharSequence arg) {
            System.err.println(arg);
          }
        },
        new Processor<CharSequence>() {
          @Override
          public void process(final CharSequence arg) {
            System.out.println(arg);
          }
        },
        new ClosureJarBuilder(LocalMREnv.DEFAULT_HOME)
    );
  }

  protected BaseEnv(final ProcessRunner runner, final String user, final String master,
                    final Processor<CharSequence> errorsProc,
                    final Processor<CharSequence> outputProc,
                    ClosureJarBuilder jarBuilder) {
    this.runner = runner;
    this.user = user;
    this.master = master;
    this.defaultErrorsProcessor = errorsProc;
    this.defaultOutputProcessor = outputProc;
    this.jarBuilder = jarBuilder;
  }

  protected void executeCommand(final List<String> options, final Processor<CharSequence> outputProcessor,
                              final Processor<CharSequence> errorsProcessor, InputStream contents) {
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


  protected final FixedSizeCache<String, MRTableShard> shardsCache = new FixedSizeCache<>(1000, CacheStrategy.Type.LRU);

  @Override
  protected void invoke(MREnv.ShardAlter e) {
    if (e.type == MREnv.ShardAlter.AlterType.CHANGED) {
      shardsCache.clear(e.shard.path());
    }
    else if (e.type == MREnv.ShardAlter.AlterType.UPDATED) {
      shardsCache.put(e.shard.path(), e.shard);
    }
    super.invoke(e);
  }

}
