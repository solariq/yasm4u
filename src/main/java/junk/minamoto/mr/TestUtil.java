package junk.minamoto.mr;

import org.jetbrains.annotations.NotNull;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.env.MRRunner;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.impl.MethodRoutineBuilder;
import ru.yandex.se.yasm4u.domains.wb.impl.StateImpl;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by minamoto on 18/09/15.
 */
public class TestUtil {
  @NotNull
  public static MRRoutineBuilder launchFakeMRReduceOperation(String testName, final InputStream in, final OutputStream out, Class<?> routineClass, String methodName, MRPath[] ins, MRPath[] outs, StateImpl state) throws IOException {
    final MethodRoutineBuilder builder = new MethodRoutineBuilder();
    builder.setRoutineClass(routineClass);
    builder.setMethodName(methodName);
    builder.setType(MRRoutineBuilder.RoutineType.REDUCE);
    builder.addInput(ins);
    builder.addOutput(outs);
    builder.setState(state);
    MRRunner runner = new MRRunner(new InputStreamReader(new GZIPInputStream(in)),
        new OutputStreamWriter(out),
        builder);
    runner.run();
    return builder;
  }
}
