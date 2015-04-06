package ru.yandex.se.lyadzhin.report;

import ru.yandex.se.yasm4u.*;
import ru.yandex.se.yasm4u.domains.wb.StateRef;

import java.util.List;

/**
 * User: lyadzhin
 * Date: 04.04.15 10:24
 */
public class ReportBLDomain implements Domain {
  public interface Output {
    StateRef<String> VP_RESULT = new StateRef<>("vp_resul", String.class);
  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }
}
