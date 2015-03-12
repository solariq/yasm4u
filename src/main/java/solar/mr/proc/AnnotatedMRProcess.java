package solar.mr.proc;

import solar.mr.MREnv;
import solar.mr.MROutput;
import solar.mr.MRRoutineBuilder;
import solar.mr.proc.impl.*;
import solar.mr.proc.tags.MRMapMethod;
import solar.mr.proc.tags.MRProcessClass;
import solar.mr.proc.tags.MRRead;
import solar.mr.proc.tags.MRReduceMethod;

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
public class AnnotatedMRProcess extends CompositeJobaBuilder {
  private Whiteboard wb;

  final static Class[] MAP_PARAMETERS_1 = {String.class, String.class, CharSequence.class, MROutput.class};
  final static Class[] MAP_PARAMETERS_2 = {MRPath.class, String.class, String.class, CharSequence.class, MROutput.class};
  final static Class[] REDUCE_PARAMETERS = {String.class, Iterator.class, MROutput.class};

  public AnnotatedMRProcess(final Class<?> processDescription, Whiteboard wb) {
    super(processDescription.getName().replace('$', '.'), resolveNames(processDescription.getAnnotation(MRProcessClass.class).goal(), wb));
    final Method[] methods = processDescription.getMethods();
    try {
      /* poka-yoke */
      processDescription.getConstructor(State.class);
    }
    catch (NoSuchMethodException nsme){
      throw new IllegalArgumentException("Missed " + processDescription.getName() + "(State)");
    }

    int ignoredMethods = 0;
    for (final Method current:methods) {
      final MRMapMethod mapAnn = current.getAnnotation(MRMapMethod.class);
      if (mapAnn != null) {
        if (!checkSignature(current, MAP_PARAMETERS_1) && !checkSignature(current,MAP_PARAMETERS_2))
          throw new RuntimeException("Invalid signature for map operation");
        addJob(new RoutineJoba(resolveNames(mapAnn.input(), wb), resolveNames(mapAnn.output(), wb), current, MRRoutineBuilder.RoutineType.MAP));
        continue;
      }
      final MRReduceMethod reduceAnn = current.getAnnotation(MRReduceMethod.class);
      if (reduceAnn != null) {
        if (!checkSignature(current, REDUCE_PARAMETERS))
          throw new RuntimeException("Invalid signature for reduce operation");
        addJob(new RoutineJoba(resolveNames(reduceAnn.input(), wb), resolveNames(reduceAnn.output(), wb), current, MRRoutineBuilder.RoutineType.REDUCE));
        continue;
      }
      final MRRead readAnn = current.getAnnotation(MRRead.class);
      if (readAnn != null) {
        addJob(new ReadJoba(resolveNames(new String[]{readAnn.input()}, wb), readAnn.output(), current));
        continue;
      }
      ignoredMethods++;
    }
    if (ignoredMethods == methods.length) {
      /* poka-yoke */
      throw new IllegalArgumentException("No annotated methods");
    }
    this.wb = wb;
  }

  private boolean checkSignature(Method current, final Class[] parameters) {
    return Arrays.equals(current.getParameterTypes(), parameters);
  }

  private static final Pattern varPattern = Pattern.compile("\\{([^\\},]+),?([^\\}]*)\\}");

  public AnnotatedMRProcess(Class<?> processDescription, MREnv env) {
    this(processDescription, new WhiteboardImpl(env, processDescription.getName()));
  }

  private static String[] resolveVars(String resource, Whiteboard vars) {
    final Matcher matcher = varPattern.matcher(resource);
    if(matcher.find()) {
      final StringBuffer format = new StringBuffer();
      String name = matcher.group(1);
      matcher.appendReplacement(format, "{" + 0 + (matcher.groupCount() > 1 && !matcher.group(2).isEmpty()? "," + matcher.group(2) : "") + "}");

      final List<String> candidates = new ArrayList<>();
      final Object resolution = vars.get(name);
      if (resolution == null)
        throw new IllegalArgumentException("No data for " + name + " available at the whiteboard");
      if (resolution.getClass().isArray()) {
        for (final Object next : (Object[]) resolution) {
          candidates.add(MessageFormat.format(format.toString(), convertToString(next)));
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

  private static Object convertToString(Object next) {
    if (next instanceof MRPath)
      return ((MRPath) next).resource().toString();
    return next;
  }

  private static String[] resolveNames(String[] input, Whiteboard wb) {
    final List<String> result = new ArrayList<>();
    for(int i = 0; i < input.length; i++) {
      result.addAll(Arrays.asList(resolveVars(input[i], wb)));
    }
    return result.toArray(new String[result.size()]);
  }

  public State execute() {
    final Joba build = build();
    if (build == null)
      throw new IllegalStateException("Unable to create a complete job!");
    build.run(wb);
    return wb.snapshot();
  }

  public Whiteboard wb() {
    return wb;
  }

  public <T> T result() {
    final Joba build = build();
    if (build == null)
      throw new IllegalStateException("Unable to create a complete job!");
    build.run(wb);
    return wb.snapshot().get(build.produces()[0]);
  }

  public String[] produces() {
    final Joba build = build();
    if (build == null)
      throw new IllegalStateException("Unable to create a complete job!");
    return build.produces();
  }
}
