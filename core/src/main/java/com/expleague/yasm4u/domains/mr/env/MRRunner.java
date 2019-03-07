package com.expleague.yasm4u.domains.mr.env;

import com.expleague.commons.io.StreamTools;
import com.expleague.commons.seq.CharSeq;
import com.expleague.commons.seq.CharSeqTools;
import com.expleague.commons.util.Holder;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.MRTools;
import com.expleague.yasm4u.domains.mr.RuntimeInterruptedException;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;
import com.expleague.yasm4u.domains.mr.ops.impl.MROperation;
import com.expleague.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.function.Predicate;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 10:41
 */
public class MRRunner implements Runnable, Serializable {
  public static final String BUILDER_RESOURCE_NAME = ".builder";

  protected final MRRoutineBuilder routineBuilder;
  protected transient final Writer out;
  protected transient final Reader in;

  public MRRunner() {
    this(new InputStreamReader(new DataInputStream(System.in), StreamTools.UTF),
        new OutputStreamWriter(System.out, StreamTools.UTF),
        readFromStream(MRRunner.class.getResourceAsStream("/" + BUILDER_RESOURCE_NAME), MRRunner.class.getClassLoader()));
  }

  @SuppressWarnings("UnusedDeclaration")
  public MRRunner(Holder<byte[]> holder) {
    final LineNumberReader in = new LineNumberReader(new InputStreamReader(System.in, StreamTools.UTF));
    this.in = in;
    this.out = new OutputStreamWriter(System.out, StreamTools.UTF);
    final ClassLoader loader = getClass().getClassLoader();
    try {
      final byte[] serializedBuilder = CharSeqTools.parseBase64(in.readLine());
//      System.err.println("holder: " + holder);
      holder.setValue(serializedBuilder);
      final ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(serializedBuilder)) {
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
      routineBuilder = (MRRoutineBuilder) is.readObject();
    }
    catch (ClassNotFoundException | IOException e) {
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
      final ObjectInputStream is = new ObjectInputStream(builderStream) {
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
      return (MRRoutineBuilder) is.readObject();
    }
    catch (ClassNotFoundException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    final MRPath[] outputTables = routineBuilder.output();
    final MROutputBase out = new MROutput2Writer(this.out, outputTables);
    try {
      final MROperation instance = routineBuilder.build(out);

      long start = System.currentTimeMillis();

      CharSeqTools.lines(in).forEach(instance.recordParser());
      instance.recordParser().accept(CharSeq.EMPTY);

      final Boolean profile = instance.state().get(ProfilerMREnv.PROFILER_ENABLED_VAR);
      if (profile != null && profile) {
        final int profilingTable = outputTables.length - 2;
        dumpProfilingStats(out, start, profilingTable);
      }
    }
    catch (RuntimeInterruptedException e) {
      // skip
    }
    catch (IOException e) {
      out.error(e, MRRecord.EMPTY);
      out.interrupt();
    }
    finally {
      /* looks very dangerous: interrupt puts STOP poison pill in the middle of MRoperation(MRMap,MRReduce) worker
       * it's seems to be acceptable for abnormal termination scenario
       */
      //out.interrupt();
      out.join();
    }
  }

  private void dumpProfilingStats(MROutputBase out, long start, int profilingTable) throws UnknownHostException {
    out.add(profilingTable, InetAddress.getLocalHost().getHostName(), "#", Long.toString(System.currentTimeMillis() - start));
  }

  public static void main(String[] args) throws Exception {
    if (args.length > 1 && "--dump".equals(args[0])) {
      buildJar(args[1], cls -> true);
    }
    else new MRRunner().run();
  }

  protected static void buildJar(String arg, Predicate<String> clsFilter) throws IOException {
    final HashMap<String, byte[]> resourcesMap = new HashMap<>();
    MRTools.buildClosureJar(MRRunner.class, arg, aClass -> {
      try {
        final Object serializedBuilderHolder = aClass.getClassLoader().loadClass(Holder.class.getName()).newInstance();
        //noinspection unchecked
        final Runnable runnable = (Runnable) aClass.getConstructor(serializedBuilderHolder.getClass()).newInstance(serializedBuilderHolder);
        resourcesMap.put(BUILDER_RESOURCE_NAME, (byte[]) serializedBuilderHolder.getClass().getMethod("getValue").invoke(serializedBuilderHolder));
        runnable.run();
      }
      catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }, clsFilter, resourcesMap);
  }
}
