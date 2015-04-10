package ru.yandex.se.lyadzhin.report.viewports;

import com.spbsu.commons.func.Computable;
import com.spbsu.commons.util.ArrayTools;
import ru.yandex.se.lyadzhin.report.sources.SourceRequest;
import ru.yandex.se.lyadzhin.report.sources.SourceResponse;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

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
    return ArrayTools.map(builder.requests(), SourceResponse.class, new Computable<SourceRequest, SourceResponse>() {
      @Override
      public SourceResponse compute(SourceRequest argument) {
        return argument.response();
      }
    });
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
