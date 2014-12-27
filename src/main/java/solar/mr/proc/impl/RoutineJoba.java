package solar.mr.proc.impl;

import com.spbsu.commons.func.Processor;
import solar.mr.MRRoutineBuilder;
import solar.mr.MRTableShard;
import solar.mr.proc.Joba;
import solar.mr.proc.Whiteboard;

import java.lang.reflect.Method;

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
    if (type == MRRoutineBuilder.RoutineType.REDUCE) {
      for (int i = 0; i < input.length; i++) {
        final String resourceName = input[i];
        wb.processAs(resourceName, new Processor<MRTableShard>() {
          @Override
          public void process(MRTableShard shard) {
            wb.env().sort(shard);
          }
        });
      }
    }

    final MethodRoutineBuilder builder = new MethodRoutineBuilder();
    builder.addInput(input);
    builder.addOutput(output);
    builder.setState(wb.snapshot());
    builder.setMethodName(method.getName());
    builder.setType(type);
    builder.setRoutineClass(method.getDeclaringClass());
    builder.complete();
    return wb.env().execute(builder, wb.errorsHandler());
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
