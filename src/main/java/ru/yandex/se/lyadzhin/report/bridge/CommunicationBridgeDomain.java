package ru.yandex.se.lyadzhin.report.bridge;

import ru.yandex.se.lyadzhin.report.cfg.Configuration;
import ru.yandex.se.lyadzhin.report.http.UserHttpCommunicationDomain;
import ru.yandex.se.lyadzhin.report.viewports.ViewportsDomain;
import ru.yandex.se.yasm4u.*;

import java.util.List;

/**
 * User: lyadzhin
 * Date: 04.04.15 11:34
 */
public class CommunicationBridgeDomain implements Domain {
  private final Configuration configuration;
  private final UserHttpCommunicationDomain communicationDomain;
  private final ViewportsDomain viewportsDomain;

  public CommunicationBridgeDomain(Configuration configuration,
                                   UserHttpCommunicationDomain communicationDomain,
                                   ViewportsDomain viewportsDomain)
  {
    this.configuration = configuration;
    this.communicationDomain = communicationDomain;
    this.viewportsDomain = viewportsDomain;
  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    final int bodyPartsCount = configuration.viewportIdList().size() + 1;
    jobs.add(new InitiateResponseStartJoba(bodyPartsCount));

    final List<String> viewportIdList = configuration.viewportIdList();
    int bodyPartNum = 0;
    for (final String viewportId : viewportIdList)
      jobs.add(new GenerateHttpBodyPartFromViewportJoba(viewportsDomain, communicationDomain, bodyPartNum++, viewportId));
    jobs.add(new GenerateHttpBodyPartFromModelJoba(viewportsDomain, communicationDomain, bodyPartNum));
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }

}
