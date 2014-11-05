package solar.mr.proc.impl;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;


import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Pair;
import solar.mr.MROutput;
import solar.mr.MRReduce;
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
      final ClassLoader loader = getClass().getClassLoader();
      CharSequence[] routinePath = CharSeqTools.split(StreamTools.readStream(loader.getResourceAsStream(MRRunner.ROUTINES_PROPERTY_NAME)), "\t");
      final Class<?> routineClass = Class.forName(routinePath[0].toString());
      routineObj = routineClass.getConstructor(MRState.class).newInstance(state);
      method = routineClass.getMethod(routinePath[1].toString(), String.class, Iterator.class, MROutput.class);
    } catch (IOException | ClassNotFoundException | InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reduce(final String key, final Iterator<Pair<String, CharSequence>> reduce) {
    try {
      method.invoke(routineObj, key, reduce, output);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
