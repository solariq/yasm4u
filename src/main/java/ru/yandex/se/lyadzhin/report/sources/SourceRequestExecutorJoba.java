package ru.yandex.se.lyadzhin.report.sources;

import com.spbsu.commons.util.ArrayTools;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

/**
* User: lyadzhin
* Date: 13.04.15 9:32
*/
class SourceRequestExecutorJoba extends Joba.Stub {
  private final SourceCommunicationDomain domain;
  private final SourceRequest sourceRequest;

  public SourceRequestExecutorJoba(SourceCommunicationDomain domain, SourceRequest sourceRequest) {
    this.domain = domain;
    this.sourceRequest = sourceRequest;
  }

  @Override
  public Ref[] consumes() {
    return ArrayTools.concat(new Ref[] {sourceRequest}, sourceRequest.dependencies());
  }

  @Override
  public Ref[] produces() {
    return new Ref[] {sourceRequest.response()};
  }

  @Override
  public void run() {
    System.out.println("Processing source request to " + sourceRequest.sourceKey());
    domain.addResponse(sourceRequest.response());
  }
}
