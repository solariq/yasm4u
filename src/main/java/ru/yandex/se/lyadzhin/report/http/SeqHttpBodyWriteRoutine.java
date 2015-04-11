package ru.yandex.se.lyadzhin.report.http;

import ru.yandex.se.yasm4u.JobExecutorService;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;

import java.util.ArrayList;
import java.util.List;

/**
* User: lyadzhin
* Date: 08.04.15 19:14
*/
class SeqHttpBodyWriteRoutine implements Routine {
  private final HttpResponse httpResponse;

  public SeqHttpBodyWriteRoutine(HttpResponse httpResponse) {
    this.httpResponse = httpResponse;
  }

  @Override
  public Joba[] buildVariants(Ref[] state, JobExecutorService jes) {
    final List<Joba> result = new ArrayList<>();

    int partsCount = 0;
    for (Ref ref : state) {
      if (ref instanceof StartResponseRef) {
        partsCount = ((StartResponseRef) ref).partsCount();
        break;
      }
    }

    if (partsCount == 0)
      return new Joba[0];

    for (Ref ref : state) {
      if (ref instanceof HttpBodyPartRef) {
        final HttpBodyPartRef bodyPartRef = (HttpBodyPartRef) ref;
        result.add(new WriteHttpBodyPartJoba(httpResponse, bodyPartRef.partNum, jes.domain(UserHttpCommunicationDomain.class)));
      }
    }
    result.add(new FinishCommunicationJoba(httpResponse, partsCount, jes.domain(UserHttpCommunicationDomain.class)));
    return result.toArray(new Joba[result.size()]);
  }

}
