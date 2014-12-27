package solar.mr.env;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;


import com.spbsu.commons.func.Action;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Pair;
import solar.mr.*;

/**
* User: solar
* Date: 23.09.14
* Time: 10:41
*/
public class MRRunner implements Runnable {
  public static final Pair<Integer, CharSequence> STOP = new Pair<Integer, CharSequence>(-1, "");
  public static final String BUILDER_RESOURCE_NAME = ".builder";

  private final MRRoutineBuilder routineBuilder;
  private final OutputStream out;
  private final Reader in;

  public MRRunner() {
    this(System.in, System.out);
  }
  public MRRunner(InputStream in, OutputStream out) {
    this.out = out;
    this.in = new InputStreamReader(in, Charset.forName("UTF-8"));

    try {
      //noinspection unchecked
      final ClassLoader loader = getClass().getClassLoader();
      final ObjectInputStream is = new ObjectInputStream(MRRunner.class.getResourceAsStream("/" + BUILDER_RESOURCE_NAME)){
        @Override
        protected Class<?> resolveClass(final ObjectStreamClass desc) throws IOException, ClassNotFoundException {
          try {
            return loader.loadClass(desc.getName());
          }
          catch (ClassNotFoundException cnfe) {
            return super.resolveClass(desc);
          }
        }
      };
      routineBuilder = (MRRoutineBuilder)is.readObject();
    } catch (ClassNotFoundException | IOException e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public void run() {
    final String[] outputTables = routineBuilder.output();
    final MROutputImpl out = new MROutputImpl(new OutputStreamWriter(this.out, StreamTools.UTF), outputTables);
    try {
      final MRRoutine instance = routineBuilder.build(out);

      long start = System.currentTimeMillis();
      CharSeqTools.processLines(in, instance);
      instance.process(CharSeq.EMPTY);
      final Boolean profile = instance.state().get(ProfilerMREnv.PROFILER_ENABLED_VAR);
      if (profile != null && profile) {
        final int profilingTable = outputTables.length - 2;
        dumpProfilingStats(out, start, profilingTable);
      }
    } catch (RuntimeInterruptedException e) {
      // skip
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      out.interrupt();
      out.join();
    }
  }

  private void dumpProfilingStats(MROutputImpl out, long start, int profilingTable) throws UnknownHostException {
    out.add(profilingTable, InetAddress.getLocalHost().getHostName(), "#", Long.toString(System.currentTimeMillis() - start));
  }

  public static void main(String[] args) throws Exception {
    if (args.length > 1 && "--dump".equals(args[0])) {
      MRTools.buildClosureJar(MRRunner.class, args[1], new Action<Class>() {
        @Override
        public void invoke(Class aClass) {
          try {
            ((Runnable)aClass.newInstance()).run();
          } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
      });
    }
    else new MRRunner().run();
  }
}
