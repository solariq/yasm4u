package solar.mr.env;

import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.impl.WeakListenerHolderImpl;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.system.RuntimeUtils;
import com.spbsu.commons.util.cache.CacheStrategy;
import com.spbsu.commons.util.cache.impl.FixedSizeCache;
import org.apache.log4j.Logger;
import solar.mr.*;
import solar.mr.routines.MRRecord;

import java.io.*;
import java.util.List;

/**
 * Created by minamoto on 10/12/14.
 */
public abstract class RemoteMREnv extends WeakListenerHolderImpl<MREnv.ShardAlter> implements MREnv {
  private static Logger LOG = Logger.getLogger(RemoteMREnv.class);
  protected final String user;

  protected final String master;
  protected final Processor<CharSequence> defaultErrorsProcessor;
  protected final Processor<CharSequence> defaultOutputProcessor;
  protected final ProcessRunner runner;

  protected RemoteMREnv(final ProcessRunner runner, final String user, final String master) {
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
        }
    );
  }

  protected RemoteMREnv(final ProcessRunner runner, final String user, final String master,
                        final Processor<CharSequence> errorsProc,
                        final Processor<CharSequence> outputProc) {
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
        outputStream.writeObject(this);
      }
      final Writer to = new OutputStreamWriter(process.getOutputStream(), StreamTools.UTF);
      final Reader from = new InputStreamReader(process.getInputStream(), StreamTools.UTF);
      to.append(CharSeqTools.toBase64(builderSerialized.toByteArray())).append("\n");
      to.flush();

      final String[] input = builder.input();
      for (int i = 0; i < input.length; i++) {
        final MRTableShard inputShard = env.resolve(input[i]);
        env.sample(inputShard, new Processor<CharSequence>() {
          @Override
          public void process(CharSequence arg) {
            try {
              to.append(arg).append("\n");
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
      }
      to.close();
      final MROutputImpl output = new MROutputImpl(env, builder.output(), new MRErrorsHandler() {
        @Override
        public void error(String type, String cause, MRRecord record) {
          throw new RuntimeException("Error during MR operation.\nType: " + type + "\tCause: " + cause + "\tRecord: [" + record + "]");
        }

        @Override
        public void error(Throwable th, MRRecord record) {
          throw new RuntimeException("Exception during processing: [" + record.toString() + "]", th);
        }
      });
      CharSeqTools.processLines(from, new Processor<CharSequence>() {
        @Override
        public void process(CharSequence arg) {
          output.parse(arg);
        }
      });

      process.waitFor();
      return jar;
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    finally {
      if (process != null)
        try {
          StreamTools.transferData(process.getErrorStream(), System.err);
        } catch (IOException e) {
          LOG.warn(e);
        }
    }
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
