package solar.mr.proc.impl;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
import solar.mr.MRTableShard;
import solar.mr.routines.MRReduce;

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
      final List<MRTableShard> inTables = new ArrayList<>();
      final boolean need2sort = MRReduce.class.isAssignableFrom(routineClass);
      for (int i = 0; i < in.length; i++) {
        final Object resolve = wb.refresh(in[i]);
        if (resolve instanceof MRTableShard) {
          MRTableShard shard = (MRTableShard) resolve;
          if (need2sort)
            wb.set(in[i], shard = wb.env().sort(shard));
          inTables.add(shard);
        }
      }
      final MRTableShard[] outTables = new MRTableShard[out.length];
      for (int i = 0; i < out.length; i++) {
        final Object resolve = wb.resolve(out[i]);
        if (resolve instanceof MRTableShard)
          outTables[i] = (MRTableShard)resolve;
        else throw new RuntimeException("MR routine can produce only MR table resources");
      }

      wb.set(MRRunner.ROUTINES_PROPERTY_NAME, Pair.create(method.getDeclaringClass().getName(), method.getName()));
      try {
        final MRState state = wb.snapshot();
        if (!wb.env().execute(routineClass, state, inTables.toArray(new MRTableShard[inTables.size()]), outTables, wb.errorsHandler()))
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
    public String[] consumes(MRWhiteboard wb) {
      final String[] result = new String[in.length];
      for (int i = 0; i < result.length; i++) {
        result[i] = wb.resolveName(in[i]);
      }
      return result;
    }

    @Override
    public String[] produces(MRWhiteboard wb) {
      final String[] result = new String[out.length];
      for (int i = 0; i < result.length; i++) {
        result[i] = wb.resolveName(out[i]);
      }
      return result;
    }

    @Override
    public String toString() {
      return "Annotated method routine: " + method.getDeclaringClass().getName() + " " + method.getName();
    }
  }
}
