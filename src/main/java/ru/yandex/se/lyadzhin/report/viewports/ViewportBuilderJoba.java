package ru.yandex.se.lyadzhin.report.viewports;

import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

/**
 * User: lyadzhin
 * Date: 10.04.15 15:36
 */
public class ViewportBuilderJoba implements Joba {
  private final ViewportsDomain viewportsDomain;
  private final ViewportBuilder builder;

  public ViewportBuilderJoba(ViewportsDomain viewportsDomain, ViewportBuilder builder) {
    this.viewportsDomain = viewportsDomain;
    this.builder = builder;
  }

  @Override
  public Ref[] consumes() {
    return builder.requests();
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
