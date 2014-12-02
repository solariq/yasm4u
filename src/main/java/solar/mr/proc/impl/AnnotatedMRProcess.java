package solar.mr.proc.impl;

import com.spbsu.commons.func.Processor;
import com.spbsu.commons.util.Pair;
import solar.mr.MREnv;
import solar.mr.MRRoutine;
import solar.mr.MRTableShard;
import solar.mr.env.MRRunner;
import solar.mr.proc.MRJoba;
import solar.mr.proc.MRWhiteboard;
import solar.mr.proc.tags.MRMapMethod;
import solar.mr.proc.tags.MRProcessClass;
import solar.mr.proc.tags.MRRead;
import solar.mr.proc.tags.MRReduceMethod;
import solar.mr.routines.MRReduce;

import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
* User: solar
* Date: 12.10.14
* Time: 13:20
*/
public class AnnotatedMRProcess extends MRProcessImpl {
  public AnnotatedMRProcess(final Class<?> processDescription, final MREnv env) {
    this(processDescription, env, new Properties());
  }

  public AnnotatedMRProcess(final Class<?> processDescription, final MREnv env, final Properties initial) {
    super(env, processDescription.getName().replace('$', '.'), resolveNames(processDescription.getAnnotation(MRProcessClass.class).goal(), initial));
    final Method[] methods = processDescription.getMethods();
    for (int i = 0; i < methods.length; i++) {
      final Method current = methods[i];
      final MRMapMethod mapAnn = current.getAnnotation(MRMapMethod.class);
      if (mapAnn != null)
        addJob(new RoutineJoba(resolveNames(mapAnn.input(), initial), resolveNames(mapAnn.output(), initial), current, MapMethod.class));
      final MRReduceMethod reduceAnn = current.getAnnotation(MRReduceMethod.class);
      if (reduceAnn != null)
        addJob(new RoutineJoba(resolveNames(reduceAnn.input(), initial), resolveNames(reduceAnn.output(), initial), current, ReduceMethod.class));
      final MRRead readAnn = current.getAnnotation(MRRead.class);
      if (readAnn != null)
        addJob(new ReadJoba(resolveNames(new String[]{readAnn.input()}, initial), readAnn.output(), current));
    }
    for (Map.Entry<Object, Object> entry : initial.entrySet()) {
      wb().set((String)entry.getKey(), entry.getValue());
    }
  }


  private static final Pattern varPattern = Pattern.compile("\\{([^\\},]+),?([^\\}]*)\\}");
  private static String[] resolveVars(String resource, Properties vars) {
    final Matcher matcher = varPattern.matcher(resource);
    if(matcher.find()) {
      final StringBuffer format = new StringBuffer();
      String name = matcher.group(1);
      matcher.appendReplacement(format, "{" + 0 + (matcher.groupCount() > 1 && !matcher.group(2).isEmpty()? "," + matcher.group(2) : "") + "}");

      final List<String> candidates = new ArrayList<>();
      final Object resolution = vars.get(name);
      if (resolution.getClass().isArray()) {
        for (final Object next : (Object[]) resolution) {
          candidates.add(MessageFormat.format(format.toString(), next));
        }
      }
      else candidates.add(MessageFormat.format(format.toString(), resolution));

      final List<String> results = new ArrayList<>();
      for (final String candidate : candidates) {
        StringBuffer candidateSB = new StringBuffer(candidate);
        matcher.appendTail(candidateSB);
        results.addAll(Arrays.asList(resolveVars(candidateSB.toString(), vars)));
      }
      return results.toArray(new String[results.size()]);
    }

    return new String[]{resource};
  }

  private static String[] resolveNames(String[] input, Properties wb) {
    final List<String> result = new ArrayList<>();
    for(int i = 0; i < input.length; i++) {
      result.addAll(Arrays.asList(resolveVars(input[i], wb)));
    }
    return result.toArray(new String[result.size()]);
  }

  private static class RoutineJoba implements MRJoba {
    private final Method method;
    private String[] in;
    private String[] out;
    private final Class<? extends MRRoutine> routineClass;

    public RoutineJoba(final String[] input, final String[] output, final Method method, final Class<? extends MRRoutine> routineClass) {
      this.method = method;
      this.routineClass = routineClass;
      in = input;
      out = output;
    }

    @Override
    public boolean run(final MRWhiteboard wb) {
      final List<MRTableShard> inTables = new ArrayList<>();
      final boolean need2sort = MRReduce.class.isAssignableFrom(routineClass);
      for (int i = 0; i < in.length; i++) {
        final String resourceName = in[i];
        wb.processAs(resourceName, new Processor<MRTableShard>() {
          @Override
          public void process(MRTableShard shard) {
            if (need2sort)
              wb.env().sort(shard);
            inTables.add(shard);
          }
        });
      }
      final MRTableShard[] outTables = new MRTableShard[out.length];
      for (int i = 0; i < out.length; i++) {
        final int index = i;
        if (!wb.processAs(out[index], new Processor<MRTableShard>() {
          @Override
          public void process(final MRTableShard arg) {
            outTables[index] = arg;
          }
        }))
          throw new RuntimeException("MR routine can produce only MR table resources");
      }

      wb.set(MRRunner.ROUTINES_PROPERTY_NAME, Pair.create(method.getDeclaringClass().getName(), method.getName()));
      try {
        return wb.env().execute(routineClass, wb.snapshot(), inTables.toArray(new MRTableShard[inTables.size()]), outTables, wb.errorsHandler());
      }
      finally {
        wb.remove(MRRunner.ROUTINES_PROPERTY_NAME);
      }
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
