package ru.yandex.se.lyadzhin.report.bridge;

import ru.yandex.se.lyadzhin.report.http.BodyPartRef;
import ru.yandex.se.lyadzhin.report.http.UserHttpCommunicationDomain;
import ru.yandex.se.lyadzhin.report.cfg.Configuration;
import ru.yandex.se.lyadzhin.report.viewports.ViewportsDomain;
import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;

import java.util.List;

/**
 * User: lyadzhin
 * Date: 04.04.15 11:34
 */
public class CommunicationBridgeDomain implements Domain {
  private final Configuration configuration;
  private final UserHttpCommunicationDomain communication;
  private final ViewportsDomain viewportsDomain;
  private final Whiteboard wb;

  public CommunicationBridgeDomain(Configuration configuration,
                                   UserHttpCommunicationDomain communication,
                                   ViewportsDomain viewportsDomain,
                                   Whiteboard wb)
  {
    this.configuration = configuration;
    this.communication = communication;
    this.viewportsDomain = viewportsDomain;
    this.wb = wb;
  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    final BodyPartRef[] bodyPartRefs = communication.allocateBodyParts(1);
    jobs.add(new ViewportModel2BodyPartConverterJoba(bodyPartRefs[0], viewportsDomain, wb));
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }


}
