package ru.yandex.se.lyadzhin.report.viewports;

import ru.yandex.se.lyadzhin.report.sources.SourceRequest;

/**
 * User: lyadzhin
 * Date: 10.04.15 14:31
 */
public interface ViewportBuilder {
  String id();

  SourceRequest[] requests();

  Viewport build();
}
