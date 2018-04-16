package junk.minamoto.mr;

import org.jetbrains.annotations.NotNull;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.env.MRRunner;
import com.expleague.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import com.expleague.yasm4u.domains.mr.routines.ann.impl.MethodRoutineBuilder;
import com.expleague.yasm4u.domains.wb.impl.StateImpl;

import java.io.*;

/**
 * Created by minamoto on 18/09/15.
 */
public class TestUtil {
  @NotNull
  public static MRRoutineBuilder launchFakeMRReduceOperation(final InputStream in, final OutputStream out, Class<?> routineClass, String methodName, MRPath[] ins, MRPath[] outs, StateImpl state) throws IOException {
    return getMrRoutineBuilder(in, out, routineClass, methodName, ins, outs, state, MRRoutineBuilder.RoutineType.REDUCE);
  }

  @NotNull
  public static MRRoutineBuilder launchFakeMRMapOperation(final InputStream in, final OutputStream out, Class<?> routineClass, String methodName, MRPath[] ins, MRPath[] outs, StateImpl state) throws IOException {
    return getMrRoutineBuilder(in, out, routineClass, methodName, ins, outs, state, MRRoutineBuilder.RoutineType.MAP);
  }

  @NotNull
  private static MRRoutineBuilder getMrRoutineBuilder(InputStream in, OutputStream out, Class<?> routineClass, String methodName, MRPath[] ins, MRPath[] outs, StateImpl state, MRRoutineBuilder.RoutineType routineType) throws IOException {
    final MethodRoutineBuilder builder = new MethodRoutineBuilder();
    builder.setRoutineClass(routineClass);
    builder.setMethodName(methodName);
    builder.setType(routineType);
    builder.addInput(ins);
    builder.addOutput(outs);
    builder.setState(state);
    MRRunner runner = new MRRunner(new InputStreamReader(in),
        new OutputStreamWriter(out),
        builder);
    runner.run();
    return builder;
  }
}
