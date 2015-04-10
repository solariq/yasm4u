package ru.yandex.se.lyadzhin.report.viewports;

import ru.yandex.se.yasm4u.Ref;

import java.net.URI;

/**
 * User: lyadzhin
 * Date: 10.04.15 14:22
 */
public class ViewportRef implements Ref<Viewport, ViewportsDomain> {
  private final String viewportId;

  public ViewportRef(String viewportId) {
    this.viewportId = viewportId;
  }

  @Override
  public URI toURI() {
    return null;
  }

  @Override
  public Class<Viewport> type() {
    return Viewport.class;
  }

  @Override
  public Class<ViewportsDomain> domainType() {
    return ViewportsDomain.class;
  }

  @Override
  public Viewport resolve(ViewportsDomain controller) {
    return controller.findViewportById(viewportId);
  }

  @Override
  public boolean available(ViewportsDomain controller) {
    return controller.findViewportById(viewportId) != null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ViewportRef that = (ViewportRef) o;

    return viewportId.equals(that.viewportId);

  }

  @Override
  public int hashCode() {
    return viewportId.hashCode();
  }
}
