package ru.yandex.se.lyadzhin.report.viewports;

import java.util.Collection;
import java.util.List;

/**
 * User: lyadzhin
 * Date: 10.04.15 14:31
 */
public interface ViewportRanker {
  List<Viewport> rank(Collection<Viewport> viewports);
}
