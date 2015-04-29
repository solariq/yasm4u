package ru.yandex.se.yasm4u.domains.mr.routines.ann;

import ru.yandex.se.yasm4u.*;
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import ru.yandex.se.yasm4u.domains.mr.routines.MergeRoutine;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.impl.ReadJoba;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.impl.RoutineJoba;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRMapMethod;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRProcessClass;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRRead;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRReduceMethod;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.wb.StateRef;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;
import ru.yandex.se.yasm4u.domains.wb.impl.WhiteboardImpl;
import ru.yandex.se.yasm4u.impl.MainThreadJES;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 13:20
 */
public class AnnotatedMRProcess implements Routine {
  public final static Class[] MAP_PARAMETERS_1 = {String.class, String.class, CharSequence.class, MROutput.class};
  public final static Class[] MAP_PARAMETERS_2 = {MRPath.class, String.class, String.class, CharSequence.class, MROutput.class};
  public final static Class[] REDUCE_PARAMETERS = {String.class, Iterator.class, MROutput.class};
  private final Class<?> processDescription;
  private final Whiteboard wb;
  private final Ref[] goals;
  private final JobExecutorService jes;
  private List<Routine> routines = new ArrayList<>();

  public AnnotatedMRProcess(final Class<?> processDescription, Whiteboard wb, MREnv env) {
    this.processDescription = processDescription;
    this.jes = new MainThreadJES(false, env, wb);
    this.jes.addRoutine(this);
    goals = resolveNames(processDescription.getAnnotation(MRProcessClass.class).goal(), jes);
    try {
      processDescription.getConstructor(State.class);
    } catch (NoSuchMethodException nsme) {
      throw new IllegalArgumentException("Missed " + processDescription.getName() + "(State)");
    }

    this.wb = wb;
  }

  public AnnotatedMRProcess(Class<?> processDescription, MREnv env) {
    this(processDescription, new WhiteboardImpl(env, processDescription.getSimpleName()), env);
  }

  @Override
  public Joba[] buildVariants(Ref[] state, JobExecutorService jes) {
    final List<Ref> input = new ArrayList<>();
    final List<Ref> output = new ArrayList<>();
    if (checkInput(state, input, output)) {
      return new Joba[]{new MyJoba(input.toArray(new Ref[input.size()]), goals, new MainThreadJES(false, jes.domains()))};
    }
    return new Joba[0];
  }

  private boolean checkInput(final Ref[] available, List<Ref> in, List<Ref> out) {
    { // trying to get input from Input interface
      final Class<?>[] classes = processDescription.getClasses();
      for (int i = 0; i < classes.length; i++) {
        final Class<?> next = classes[i];
        if ("Input".equals(next.getSimpleName()) && next.isInterface()) {
          final Field[] fields = next.getFields();
          for (int j = 0; j < fields.length; j++) {
            final Field f = fields[j];
            boolean found = false;
            try {
              final Object inputRef = f.get(null);
              for (int k = 0; !found && k < available.length; k++) {
                found = inputRef.equals(available[k]);
              }
            } catch (IllegalAccessException e) {
              throw new RuntimeException("Never happen " + e);
            }
            if (!found)
              return false;
          }
          return true;
        }
      }
    }

    { // fallback to old interface
      final Method[] methods = processDescription.getMethods();
      for (final Method current : methods) {
        String[] input = new String[0];
        String[] output = input;
        final MRMapMethod mapAnn = current.getAnnotation(MRMapMethod.class);
        if (mapAnn != null) {
          input = mapAnn.input();
          output = mapAnn.output();
        }
        final MRReduceMethod reduceAnn = current.getAnnotation(MRReduceMethod.class);
        if (reduceAnn != null) {
          input = reduceAnn.input();
          output = reduceAnn.output();
        }
        final MRRead readAnn = current.getAnnotation(MRRead.class);
        if (readAnn != null) {
          input = new String[]{readAnn.input()};
          output = new String[]{readAnn.output()};
        }
        for (int i = 0; i < input.length; i++) {
          if (!checkVars(input[i], available, in))
            return false;
        }
        for (int i = 0; i < output.length; i++) {
          if (!checkVars(output[i], available, out))
            return false;
        }
      }
    }
    return true;
  }

  private boolean checkVars(String resource, Ref[] available, List<Ref> vars) {
    final Matcher matcher = varPattern.matcher(resource);
    while (matcher.find()) {
      final String name = matcher.group(1);
      final Ref var = jes.parse(name);
      vars.add(var);
      boolean found = false;
      for (int i = 0; !found && i < available.length; i++) {
        found = var.equals(available[i]);
      }
      if (!found)
        return false;
    }
    return true;
  }

  private static final Pattern varPattern = Pattern.compile("\\{([^\\},]+),?([^\\}]*)\\}");

  private static Object convertToString(Object next) {
    if (next instanceof MRPath)
      return ((MRPath) next).toURI().toString();
    return next;
  }

  private static Ref[] resolveVars(String resource, Domain.Controller jes) {
    final Matcher matcher = varPattern.matcher(resource);
    if (matcher.find()) {
      final StringBuffer format = new StringBuffer();
      String name = matcher.group(1);
      matcher.appendReplacement(format, "{" + 0 + (matcher.groupCount() > 1 && !matcher.group(2).isEmpty() ? "," + matcher.group(2) : "") + "}");
      final MessageFormat fmt = new MessageFormat(format.toString(), Locale.ENGLISH);

      final List<String> candidates = new ArrayList<>();

      final Ref convert = jes.parse(name);
      final Object resolution = jes.resolve(convert);
      if (resolution == null)
        throw new IllegalArgumentException("No data for " + name + " available at the whiteboard");
      if (resolution.getClass().isArray()) {
        for (final Object next : (Object[]) resolution) {
          candidates.add(fmt.format(new Object[]{convertToString(next)}));
        }
      } else candidates.add(fmt.format(new Object[]{resolution}));

      final List<Ref> results = new ArrayList<>();
      for (final String candidate : candidates) {
        StringBuffer candidateSB = new StringBuffer(candidate);
        matcher.appendTail(candidateSB);
        results.addAll(Arrays.asList(resolveVars(candidateSB.toString(), jes)));
      }
      return results.toArray(new Ref[results.size()]);
    }
    final Ref ref = jes.parse(resource);
    if (MRPath.class.isAssignableFrom(ref.type())) {
      final MRPath resolve = (MRPath) jes.resolve(ref);
      if (resolve.path.endsWith("*")) {
        final MRPath dir = resolve.parent();
        if (dir.isDirectory())
          return jes.domain(MREnv.class).list(dir);
      }
    }
    return new Ref[]{ref};
  }

  private static Ref[] resolveNames(String[] input, Domain.Controller jes) {
    final List<Ref> result = new ArrayList<>();
    for (int i = 0; i < input.length; i++) {
      result.addAll(Arrays.asList(resolveVars(input[i], jes)));
    }
    return result.toArray(new Ref[result.size()]);
  }

  public void execute() {
    result();
  }

  public Whiteboard wb() {
    return wb;
  }

  public <T> T result() {
    final Ref[] goals = goals();
    final Future<List<?>> calculate = jes.calculate(goals);
    try {
      //noinspection unchecked
      return (T) calculate.get().get(0);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public Ref[] goals() {
    return this.goals;
  }

  public String[] produces() {
    final String[] result = new String[goals.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = goals[i].toURI().toString();
    }
    return result;
  }

  public void addRoutine(Routine routine) {
    routines.add(routine);
  }

  private class MyJoba extends Joba.Stub {
    private final JobExecutorService jes;

    public MyJoba(Ref[] in, Ref[] out, JobExecutorService jes) {
      super(in, out);
      this.jes = jes;
    }

    @Override
    public void run() {
      final Method[] methods = processDescription.getMethods();
      List<RoutineJoba> jobs = new ArrayList<>();
      for (final Method current : methods) {
        final MRMapMethod mapAnn = current.getAnnotation(MRMapMethod.class);
        if (mapAnn != null) {
          if (!Arrays.equals(current.getParameterTypes(), MAP_PARAMETERS_1) && !Arrays.equals(current.getParameterTypes(), MAP_PARAMETERS_2))
            throw new RuntimeException("Invalid signature for map operation");
          //noinspection unchecked
          jobs.add(new RoutineJoba(jes, resolveNames(mapAnn.input(), jes), resolveNames(mapAnn.output(), jes), current, MRRoutineBuilder.RoutineType.MAP));
        }
        final MRReduceMethod reduceAnn = current.getAnnotation(MRReduceMethod.class);
        if (reduceAnn != null) {
          if (!Arrays.equals(current.getParameterTypes(), REDUCE_PARAMETERS))
            throw new RuntimeException("Invalid signature for reduce operation");
          //noinspection unchecked
          jobs.add(new RoutineJoba(jes, resolveNames(reduceAnn.input(), jes), resolveNames(reduceAnn.output(), jes), current, MRRoutineBuilder.RoutineType.REDUCE));
        }
        final MRRead readAnn = current.getAnnotation(MRRead.class);
        if (readAnn != null) {
          final Ref convert = jes.parse(readAnn.output());
          if (!(convert instanceof StateRef))
            throw new IllegalStateException("Reader result must be instance of StateRef");
          //noinspection unchecked
          jes.addJoba(new ReadJoba(jes, resolveNames(new String[]{readAnn.input()}, jes), (StateRef<?>) convert, current));
        }
      }
      jobs = MergeRoutine.unmergeJobs(jobs);
      for (RoutineJoba job : jobs) {
        jes.addJoba(job);
      }
      for (Routine routine : routines) {
        jes.addRoutine(routine);
      }
      final Future<List<?>> future = jes.calculate(produces());
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String toString() {
      return "Composite joba based on: " + processDescription.getSimpleName();
    }
  }
}
