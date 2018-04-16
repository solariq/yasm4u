package com.expleague.lyadzhin.report.viewports;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
* User: lyadzhin
* Date: 11.04.15 0:08
*/
class MockViewportRanker implements ViewportRanker {
  @Override
  public List<Viewport> rank(Collection<Viewport> viewports) {
    return new ArrayList<>(viewports);
  }
}
