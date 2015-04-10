package ru.yandex.se.lyadzhin.report.viewports;

import ru.yandex.se.lyadzhin.report.cfg.Configuration;
import ru.yandex.se.lyadzhin.report.sources.SourceRequest;
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
      jobs.add(new ViewportBuilderJoba(this, new MyViewportBuilder(viewportId)));
    }
    jobs.add(new ViewportRankerJoba(this, new MyViewportRanker(), configuration.viewportIdList()));
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

  private static class MyViewportBuilder implements ViewportBuilder {
    private final String viewportId;

    public MyViewportBuilder(String viewportId) {
      this.viewportId = viewportId;
    }

    @Override
    public String id() {
      return viewportId;
    }

    @Override
    public SourceRequest[] requests() {
      return new SourceRequest[] {new SourceRequest("IMAGES_SEARCH")};
    }

    @Override
    public Viewport build() {
      return new Viewport() {
        @Override
        public String id() {
          return viewportId;
        }

        @Override
        public String toString() {
          return "Viewport{" +
                  "viewportId='" + viewportId + '\'' +
                  '}';
        }
      };
    }

    @Override
    public String toString() {
      return "MyViewportBuilder{" +
              "viewportId='" + viewportId + '\'' +
              '}';
    }
  }

  private static class MyViewportRanker implements ViewportRanker {
    @Override
    public List<Viewport> rank(Collection<Viewport> viewports) {
      return new ArrayList<>(viewports);
    }
  }
}
