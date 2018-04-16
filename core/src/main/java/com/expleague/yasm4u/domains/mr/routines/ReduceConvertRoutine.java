package com.expleague.yasm4u.domains.mr.routines;

import com.expleague.yasm4u.JobExecutorService;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.Routine;
import com.expleague.yasm4u.domains.mr.MROutput;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import com.expleague.yasm4u.domains.mr.routines.ann.AnnotatedMRProcess;
import com.expleague.yasm4u.domains.mr.routines.ann.impl.RoutineJoba;
import org.jetbrains.annotations.Nullable;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * User: solar
 * Date: 07.11.14
 * Time: 16:39
 */
@SuppressWarnings("UnusedDeclaration")
public abstract class ReduceConvertRoutine implements Routine {
  public abstract void convert(String key, Iterator<MRRecord> iter, MROutput out);

  @Nullable
  protected abstract MRPath convertPath(MRPath path);

  @Override
  public Joba[] buildVariants(Ref[] state, final JobExecutorService executor) {
    final List<Joba> variants = new ArrayList<>();
    for (final Ref ref : state) {
      if (MRPath.class.isAssignableFrom(ref.type())) {
        final MRPath input = (MRPath) executor.resolve(ref);
        final MRPath output = convertPath(input);
        if (output != null) {
          try {
            //noinspection unchecked
            variants.add(new RoutineJoba(executor, new Ref[]{input}, new Ref[]{output}, getClass().getMethod("convert", AnnotatedMRProcess.REDUCE_PARAMETERS), MRRoutineBuilder.RoutineType.REDUCE));
          } catch (NoSuchMethodException e) {
            throw new RuntimeException("This must not happen, someone changed the class structure!", e);
          }
        }
      }
    }
    return variants.toArray(new Joba[variants.size()]);
  }
}
