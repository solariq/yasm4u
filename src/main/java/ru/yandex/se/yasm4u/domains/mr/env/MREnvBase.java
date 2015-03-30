package ru.yandex.se.yasm4u.domains.mr.env;

import com.spbsu.commons.func.Action;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.mr.routines.MergeRoutine;
import ru.yandex.se.yasm4u.domains.mr.routines.SortRoutine;

/**
 * User: solar
 * Date: 27.03.15
 * Time: 16:51
 */
public abstract class MREnvBase implements MREnv {
  @Override
  public Routine[] publicRoutines() {
    return new Routine[]{new MergeRoutine(), new SortRoutine()};
  }
}
