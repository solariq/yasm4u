package solar.mr.env;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Holder;
import com.spbsu.commons.util.Pair;
import solar.mr.MRRoutine;
import solar.mr.MRRoutineBuilder;
import solar.mr.MRTools;
import solar.mr.RuntimeInterruptedException;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

/**
* User: solar
* Date: 23.09.14
* Time: 10:41
*/
public class MRRunner implements Runnable {
  public static final Pair<Integer, CharSequence> STOP = new Pair<Integer, CharSequence>(-1, "");
  public static final String BUILDER_RESOURCE_NAME = ".builder";

  private final MRRoutineBuilder routineBuilder;
  private final Writer out;
  private final Reader in;

  public MRRunner() {
    this(new InputStreamReader(System.in, StreamTools.UTF),
         new OutputStreamWriter(System.out, StreamTools.UTF),
         readFromStream(MRRunner.class.getResourceAsStream("/" + BUILDER_RESOURCE_NAME), MRRunner.class.getClassLoader()));
  }

  public MRRunner(Reader in, Writer out, MRRoutineBuilder builder) {
    this.out = out;
    this.in = in;
    this.routineBuilder = builder;
  }

  public MRRunner(Holder<byte[]> holder) {
    final LineNumberReader in = new LineNumberReader(new InputStreamReader(System.in, StreamTools.UTF));
    this.in = in;
    this.out = new OutputStreamWriter(System.out, StreamTools.UTF);
    final ClassLoader loader = getClass().getClassLoader();
    try {
      final byte[] serializedBuilder = CharSeqTools.parseBase64(in.readLine());
//      System.err.println("holder: " + holder);
      holder.setValue(serializedBuilder);
      final ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(serializedBuilder)){
        @Override
        protected Class<?> resolveClass(final ObjectStreamClass desc) throws IOException, ClassNotFoundException {
          try {
//            System.err.println(desc.getName());
            return loader.loadClass(desc.getName());
          }
          catch (ClassNotFoundException cnfe) {
            return super.resolveClass(desc);
          }
        }
      };
      return  (MRRoutineBuilder)is.readObject();
    } catch (ClassNotFoundException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public MRRunner(Reader in, Writer out, MRRoutineBuilder builder) {
    this.out = out;
    this.in = in;
    this.routineBuilder = builder;
  }


  private static MRRoutineBuilder readFromStream(InputStream builderStream, final ClassLoader loader) {
    try {
      //noinspection unchecked
      final ObjectInputStream is = new ObjectInputStream(builderStream){
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
      return  (MRRoutineBuilder)is.readObject();
    } catch (ClassNotFoundException | IOException e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public void run() {
    final String[] outputTables = routineBuilder.output();
    final MROutputImpl out = new MROutputImpl(this.out, outputTables);
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
      final HashMap<String, byte[]> resourcesMap = new HashMap<>();
      MRTools.buildClosureJar(MRRunner.class, args[1], new Action<Class>() {
        @Override
        public void invoke(Class aClass) {
          try {
            final Object serializedBuilderHolder = aClass.getClassLoader().loadClass(Holder.class.getName()).newInstance();
            final Runnable runnable = (Runnable) aClass.getConstructor(serializedBuilderHolder.getClass()).newInstance(serializedBuilderHolder);
            resourcesMap.put(BUILDER_RESOURCE_NAME, (byte[])serializedBuilderHolder.getClass().getMethod("getValue").invoke(serializedBuilderHolder));
            runnable.run();
          } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        }
      }, resourcesMap);
    }
    else new MRRunner().run();
  }
}
