package ru.yandex.se.lyadzhin.report;

import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.JobExecutorService;
import ru.yandex.se.yasm4u.domains.wb.StateRef;

/**
 * User: lyadzhin
 * Date: 04.04.15 10:24
 */
public class ReportBLDomain implements Domain {
  public interface Output {
    StateRef<String> VP_RESULT = new StateRef<>("vp_resul", String.class);
  }

  @Override
  public void init(JobExecutorService jes) {

  }
}
