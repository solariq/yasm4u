package solar.mr.proc.impl;

import com.spbsu.commons.func.Processor;
import solar.mr.MRRoutineBuilder;
import solar.mr.MRTableState;
import solar.mr.proc.Joba;
import solar.mr.proc.Whiteboard;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
* User: solar
* Date: 27.12.14
* Time: 13:02
*/
public class RoutineJoba implements Joba {
  private final String[] input;
  private final String[] output;
  private final Method method;
  private final MRRoutineBuilder.RoutineType type;

  public RoutineJoba(final String[] input, final String[] output, final Method method, MRRoutineBuilder.RoutineType type) {
//    if (type == MRRoutineBuilder.RoutineType.REDUCE) {
//      for(int i = 0; i < input.length; i++) {
//        input[i] += "?sorted=true";
//      }
//    }
    this.input = input;
    this.output = output;
    this.method = method;
    this.type = type;
  }

  @Override
  public String name() {
    return toString();
  }

  @Override
  public boolean run(final Whiteboard wb) {
    final MRPath[] inputResolved = resolveAll(input, wb);
    final MRPath[] outputResolved = resolveAll(output, wb);
    if (type == MRRoutineBuilder.RoutineType.REDUCE) {
      for (int i = 0; i < inputResolved.length; i++) {
        final MRPath resourceName = inputResolved[i];
        if (!resourceName.sorted) {
          wb.env().sort(resourceName);
          final MRPath sortedPath = new MRPath(resourceName.mount, resourceName.path, true);
          inputResolved[i] = sortedPath;
        }
      }
    }

    final MethodRoutineBuilder builder = new MethodRoutineBuilder();
    builder.addInput(inputResolved);
    builder.addOutput(outputResolved);
    builder.setState(wb.snapshot());
    builder.setMethodName(method.getName());
    builder.setType(type);
    builder.setRoutineClass(method.getDeclaringClass());
    return wb.env().execute(builder, wb.errorsHandler());
  }

  private MRPath[] resolveAll(String[] input, Whiteboard wb) {
    final List<MRPath> result = new ArrayList<>();
    for(int i = 0; i < input.length; i++) {
      wb.processAs(input[i], new Processor<MRPath>() {
        @Override
        public void process(MRPath arg) {
          result.add(arg);
        }
      });
    }
    return result.toArray(new MRPath[result.size()]);
  }

  @Override
  public String[] consumes() {
    return input;
  }

  @Override
  public String[] produces() {
    return output;
  }

  @Override
  public String toString() {
    return "Annotated method routine: " + method.getDeclaringClass().getName() + " " + method.getName();
  }

}
