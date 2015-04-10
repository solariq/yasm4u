package ru.yandex.se.lyadzhin.report.sources;

import ru.yandex.se.yasm4u.JobExecutorService;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;

import java.util.ArrayList;
import java.util.List;

/**
* User: lyadzhin
* Date: 08.04.15 19:18
*/
class PublishSourceRequestExecutorsRoutine implements Routine {
  @Override
  public Joba[] buildVariants(Ref[] state, JobExecutorService executor) {
    final List<Joba> result = new ArrayList<>();
    for (final Ref ref : state) {
      if (ref instanceof SourceRequest)
        result.add(new RequestExecutorJoba((SourceRequest) ref));
    }
    return result.toArray(new Joba[result.size()]);
  }

  private static class RequestExecutorJoba implements Joba {
    private final SourceRequest sourceRequest;

    public RequestExecutorJoba(SourceRequest sourceRequest) {
      this.sourceRequest = sourceRequest;
    }

    @Override
    public Ref[] consumes() {
      return new Ref[] {sourceRequest};
    }

    @Override
    public Ref[] produces() {
      return new Ref[] {sourceRequest.response()};
    }

    @Override
    public void run() {
      System.out.println("Processing source request to " + sourceRequest);
    }
  }
}
