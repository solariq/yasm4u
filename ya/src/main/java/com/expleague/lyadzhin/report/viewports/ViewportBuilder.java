package com.expleague.lyadzhin.report.viewports;

import com.expleague.lyadzhin.report.sources.SourceRequest;

/**
 * User: lyadzhin
 * Date: 10.04.15 14:31
 */
public interface ViewportBuilder {
  String id();

  SourceRequest[] requests();

  Viewport build();
}
