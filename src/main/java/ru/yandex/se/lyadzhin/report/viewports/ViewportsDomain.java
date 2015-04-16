package ru.yandex.se.lyadzhin.report.viewports;

import ru.yandex.se.lyadzhin.report.cfg.Configuration;
import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;

import java.net.URI;
import java.util.*;

/**
 * User: lyadzhin
 * Date: 04.04.15 10:24
 */
public class ViewportsDomain implements Domain {
  public static final Ref<ViewportsModel,ViewportsDomain> REF_VIEWPORTS_MODEL = new ViewportsModelRef();

  private final Configuration configuration;

  private final Map<String,Viewport> viewports = new HashMap<>();
  private ViewportsModel viewportsModel;

  public ViewportsDomain(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    for (String viewportId : configuration.viewportIdList()) {
      final MockViewportBuilder viewportBuilder = new MockViewportBuilder(viewportId);
      jobs.add(new ViewportRequestingJoba(viewportBuilder));
      jobs.add(new ViewportBuildingJoba(this, viewportBuilder));
    }
    jobs.add(new ViewportRankingJoba(this, new MockViewportRanker(), configuration.viewportIdList()));
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }

  public ViewportsModel getViewportsModel() {
    return viewportsModel;
  }

  public Viewport findViewportById(String viewportId) {
    return viewports.get(viewportId);
  }

  void setViewportsModel(ViewportsModel viewportsModel) {
    this.viewportsModel = viewportsModel;
  }

  void onBuilderSuccess(String viewportId, Viewport viewport) {
    viewports.put(viewportId, viewport);
  }

  void onBuilderFailed(String viewportId) {
    viewports.put(viewportId, null);
  }

  Collection<Viewport> getViewports() {
    final ArrayList<Viewport> result = new ArrayList<>(viewports.values());
    for (Viewport viewport : viewports.values()) {
      if (viewport != null) {
        result.add(viewport);
      }
    }
    return result;
  }

  boolean isBuildingCompleted(String viewportId) {
    return viewports.containsKey(viewportId);
  }

  private static class ViewportsModelRef implements Ref<ViewportsModel,ViewportsDomain> {
    @Override
    public URI toURI() {
      return null;
    }

    @Override
    public Class<ViewportsModel> type() {
      return ViewportsModel.class;
    }

    @Override
    public Class<ViewportsDomain> domainType() {
      return ViewportsDomain.class;
    }

    @Override
    public ViewportsModel resolve(ViewportsDomain controller) {
      return controller.viewportsModel;
    }

    @Override
    public boolean available(ViewportsDomain controller) {
      return controller.viewportsModel != null;
    }
  }

}
