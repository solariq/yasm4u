package ru.yandex.se.yasm4u.domains.mr.env;

import ru.yandex.se.yasm4u.JobExecutorService;
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
  public void init(JobExecutorService jes) {
    jes.addRoutine(new MergeRoutine());
    jes.addRoutine(new SortRoutine());
  }
}
