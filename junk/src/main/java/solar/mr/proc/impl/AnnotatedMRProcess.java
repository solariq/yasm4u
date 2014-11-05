package solar.mr.proc.impl;

import java.lang.reflect.Method;


import com.spbsu.commons.util.Pair;
import solar.mr.MREnv;
import solar.mr.MRRoutine;
import solar.mr.env.MRRunner;
import solar.mr.proc.MRJoba;
import solar.mr.proc.MRState;
import solar.mr.proc.MRWhiteboard;
import solar.mr.proc.tags.MRMapMethod;
import solar.mr.proc.tags.MRProcessClass;
import solar.mr.proc.tags.MRRead;
import solar.mr.proc.tags.MRReduceMethod;
import solar.mr.tables.MRTableShard;

/**
* User: solar
* Date: 12.10.14
* Time: 13:20
*/
public class AnnotatedMRProcess extends MRProcessImpl {
  public AnnotatedMRProcess(final Class<?> processDescription, MREnv env) {
    super(env, processDescription.getName().replace('$', '.'), processDescription.getAnnotation(MRProcessClass.class).goal());
    final Method[] methods = processDescription.getMethods();
    for (int i = 0; i < methods.length; i++) {
      final Method current = methods[i];
      final MRMapMethod mapAnn = current.getAnnotation(MRMapMethod.class);
      if (mapAnn != null)
        addJob(new RoutineJoba(mapAnn, current));
      final MRReduceMethod reduceAnn = current.getAnnotation(MRReduceMethod.class);
      if (reduceAnn != null)
        addJob(new RoutineJoba(reduceAnn, current));
      final MRRead readAnn = current.getAnnotation(MRRead.class);
      if (readAnn != null)
        addJob(new ReadJoba(readAnn, current));
    }
  }

  private static class RoutineJoba implements MRJoba {
    private final Method method;
    private String[] in;
    private String[] out;
    private final Class<? extends MRRoutine> routineClass;

    public RoutineJoba(final MRMapMethod mapAnn, Method method) {
      this.method = method;
      routineClass = MapMethod.class;
      in = mapAnn.input();
      out = mapAnn.output();
    }

    public RoutineJoba(final MRReduceMethod reduceAnn, Method method) {
      this.method = method;
      routineClass = ReduceMethod.class;
      in = reduceAnn.input();
      out = reduceAnn.output();
    }

    @Override
    public boolean run(final MRWhiteboard wb) {
      final MRTableShard[] inTables = new MRTableShard[in.length];
      for (int i = 0; i < in.length; i++) {
        inTables[i] = wb.resolve(in[i]);
      }
      final MRTableShard[] outTables = new MRTableShard[out.length];
      for (int i = 0; i < out.length; i++) {
        outTables[i] = wb.resolve(out[i]);
      }

      wb.set(MRRunner.ROUTINES_PROPERTY_NAME, Pair.create(method.getDeclaringClass().getName(), method.getName()));
      try {
        final MRState state = wb.snapshot();
        if (!wb.env().execute(routineClass, state, inTables, outTables, wb.errorsHandler()))
          return false;
        for (int i = 0; i < out.length; i++) {
          wb.refresh(out[i]);
        }
      }
      finally {
        wb.remove(MRRunner.ROUTINES_PROPERTY_NAME);
      }
      final MRState state = wb.snapshot();
      for (int i = 0; i < out.length; i++) {
        if (!state.available(out[i]))
          return false;
      }
      return true;
    }

    @Override
    public String[] consumes() {
      return in;
    }

    @Override
    public String[] produces() {
      return out;
    }

    @Override
    public String toString() {
      return "Annotated method routine: " + method.getDeclaringClass().getName() + " " + method.getName();
    }
  }
}
