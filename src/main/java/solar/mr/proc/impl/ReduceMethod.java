package solar.mr.proc.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;


import com.spbsu.commons.util.Pair;
import solar.mr.MROutput;
import solar.mr.routines.MRRecord;
import solar.mr.routines.MRReduce;
import solar.mr.env.MRRunner;
import solar.mr.proc.MRState;

/**
* User: solar
* Date: 13.10.14
* Time: 10:33
*/
public class ReduceMethod extends MRReduce {
  private final Method method;
  private final Object routineObj;

  public ReduceMethod(final String[] input, final MROutput output, final MRState state) {
    super(input, output, state);
    try {
      final Pair<String,String> classMethodPair = state.get(MRRunner.ROUTINES_PROPERTY_NAME);
      assert classMethodPair != null;
      final Class<?> routineClass = Class.forName(classMethodPair.getFirst());
      routineObj = routineClass.getConstructor(MRState.class).newInstance(state);
      method = routineClass.getMethod(classMethodPair.getSecond(), String.class, Iterator.class, MROutput.class);
    } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reduce(final String key, final Iterator<MRRecord> reduce) {
    try {
      method.invoke(routineObj, key, reduce, output);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
