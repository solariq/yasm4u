package ru.yandex.se.yasm4u.domains.mr.routines;

import org.jetbrains.annotations.Nullable;
import ru.yandex.se.yasm4u.JobExecutorService;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;
import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.AnnotatedMRProcess;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.impl.RoutineJoba;

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
