package com.expleague.lyadzhin.report.viewports;

import com.expleague.lyadzhin.report.cfg.Configuration;
import com.expleague.yasm4u.Domain;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.Routine;

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

  void setViewportsModel(ViewportsModel viewportsModel) {
    this.viewportsModel = viewportsModel;
  }

  void addViewport(Viewport viewport) {
    viewports.put(viewport.id(), viewport);
  }

  Collection<Viewport> getViewports() {
    return viewports.values();
  }

  Viewport findViewportById(String viewportId) {
    return viewports.get(viewportId);
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
