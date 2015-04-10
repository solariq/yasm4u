package ru.yandex.se.lyadzhin.report.viewports;

import com.spbsu.commons.func.Computable;
import com.spbsu.commons.util.CollectionTools;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

import java.util.Collection;
import java.util.List;

/**
 * User: lyadzhin
 * Date: 10.04.15 16:14
 */
public class ViewportRankingJoba implements Joba {
  private final ViewportsDomain viewportsDomain;
  private final ViewportRanker ranker;
  private final List<String> viewportIds;

  public ViewportRankingJoba(ViewportsDomain viewportsDomain, ViewportRanker ranker, List<String> viewportIds) {
    this.viewportsDomain = viewportsDomain;
    this.ranker = ranker;
    this.viewportIds = viewportIds;
  }

  @Override
  public Ref[] consumes() {
    final List<Ref> viewportRefs = CollectionTools.map(new Computable<String, Ref>() {
      @Override
      public Ref compute(String argument) {
        return new ViewportRef(argument);
      }
    }, viewportIds);
    return viewportRefs.toArray(new Ref[viewportRefs.size()]);
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {ViewportsDomain.REF_VIEWPORTS_MODEL};
  }

  @Override
  public void run() {
    System.out.println("Ranking viewports");
    final Collection<Viewport> viewports = viewportsDomain.getViewports();
    final List<Viewport> rankedViewports = ranker.rank(viewports);
    viewportsDomain.setViewportsModel(new ViewportsModel(rankedViewports));
  }
}
