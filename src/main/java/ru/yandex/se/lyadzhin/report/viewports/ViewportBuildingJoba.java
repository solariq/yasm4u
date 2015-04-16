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
public class ViewportBuildingJoba extends Joba.Stub {
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
      public SourceResponse compute(SourceRequest request) {
        return request.response();
      }
    });
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {new ViewportRef(builder.id())};
  }

  @Override
  public void run() {
    final Viewport viewport = builder.build();
    if (viewport != null) {
      System.out.println("Built viewport " + builder.id());
      viewportsDomain.onBuilderSuccess(builder.id(), viewport);
    } else {
      System.out.println("Failed to build viewport " + builder.id());
      viewportsDomain.onBuilderFailed(builder.id());
    }

  }
}
