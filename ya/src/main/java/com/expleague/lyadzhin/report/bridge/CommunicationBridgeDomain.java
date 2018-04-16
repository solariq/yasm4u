package com.expleague.lyadzhin.report.bridge;

import com.expleague.lyadzhin.report.cfg.Configuration;
import com.expleague.lyadzhin.report.http.UserHttpCommunicationDomain;
import com.expleague.yasm4u.Domain;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.Routine;
import com.expleague.yasm4u.domains.wb.Whiteboard;
import com.expleague.lyadzhin.report.http.BodyPartRef;
import com.expleague.lyadzhin.report.viewports.ViewportsDomain;

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
