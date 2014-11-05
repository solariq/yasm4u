package solar.mr.proc.impl;

import java.lang.reflect.Method;


import com.spbsu.commons.util.Pair;
import solar.mr.MREnv;
import solar.mr.MRRoutine;
import solar.mr.MRTable;
import solar.mr.env.MRRunner;
import solar.mr.proc.MRJoba;
import solar.mr.proc.MRState;
import solar.mr.proc.MRWhiteboard;
import solar.mr.proc.tags.MRMapMethod;
import solar.mr.proc.tags.MRProcessClass;
import solar.mr.proc.tags.MRRead;
import solar.mr.proc.tags.MRReduceMethod;

/**
* User: solar
* Date: 12.10.14
* Time: 13:20
*/
public class AnnotatedMRProcess extends MRProcessImpl {
  public AnnotatedMRProcess(final Class<?> processDescription, MREnv env) {
    super(env, processDescription.getName(), processDescription.getAnnotation(MRProcessClass.class).goal());
    final Method[] methods = getClass().getMethods();
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
      final MRTable[] inTables = new MRTable[in.length];
      for (int i = 0; i < in.length; i++) {
        String current = in[i];
        inTables[i] = wb.resolve(current);
      }
      final MRTable[] outTables = new MRTable[out.length];
      for (int i = 0; i < out.length; i++) {
        String current = out[i];
        outTables[i] = wb.resolve(current);
      }

      wb.set(MRRunner.ROUTINES_PROPERTY_NAME, Pair.create(method.getDeclaringClass().getName(), method.getName()));
      final MRState state = wb.slice();
      wb.remove(MRRunner.ROUTINES_PROPERTY_NAME);
      if (!wb.env().execute(routineClass, state, inTables, outTables, null))
        return false;
      for (int i = 0; i < outTables.length; i++) {
        if (!outTables[i].available(wb.env()))
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
  }
}
