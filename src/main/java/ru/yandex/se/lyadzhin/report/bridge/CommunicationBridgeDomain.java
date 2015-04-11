package ru.yandex.se.lyadzhin.report.bridge;

import ru.yandex.se.lyadzhin.report.http.UserHttpCommunicationDomain;
import ru.yandex.se.lyadzhin.report.viewports.ViewportsDomain;
import ru.yandex.se.yasm4u.*;

import java.util.List;

/**
 * User: lyadzhin
 * Date: 04.04.15 11:34
 */
public class CommunicationBridgeDomain implements Domain {
  private final UserHttpCommunicationDomain communicationDomain;
  private final ViewportsDomain viewportsDomain;

  public CommunicationBridgeDomain(UserHttpCommunicationDomain communicationDomain,
                                   ViewportsDomain viewportsDomain)
  {
    this.communicationDomain = communicationDomain;
    this.viewportsDomain = viewportsDomain;
  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    jobs.add(new GenerateHttpBodyPartFromViewportJoba(viewportsDomain, communicationDomain));
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }
}
