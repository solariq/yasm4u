package com.expleague.lyadzhin.report.viewports;

import com.expleague.commons.util.ArrayTools;
import com.expleague.lyadzhin.report.sources.SourceRequest;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.lyadzhin.report.sources.SourceResponse;

/**
 * User: lyadzhin
 * Date: 10.04.15 15:36
 */
public class ViewportBuildingJoba implements Joba {
  private final ViewportsDomain viewportsDomain;
  private final ViewportBuilder builder;

  public ViewportBuildingJoba(ViewportsDomain viewportsDomain, ViewportBuilder builder) {
    this.viewportsDomain = viewportsDomain;
    this.builder = builder;
  }

  @Override
  public Ref[] consumes() {
    return ArrayTools.map(builder.requests(), SourceResponse.class, SourceRequest::response);
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {new ViewportRef(builder.id())};
  }

  @Override
  public void run() {
    System.out.println("Building viewport " + builder.id());
    final Viewport viewport = builder.build();
    if (viewport != null)
      viewportsDomain.addViewport(viewport);

  }
}
