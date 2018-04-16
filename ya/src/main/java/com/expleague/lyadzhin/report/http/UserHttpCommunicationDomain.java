package com.expleague.lyadzhin.report.http;

import com.expleague.yasm4u.Domain;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.Routine;
import com.expleague.yasm4u.domains.wb.StateRef;
import com.expleague.yasm4u.domains.wb.Whiteboard;

import java.util.List;

/**
 * User: lyadzhin
 * Date: 04.04.15 10:46
 */
public class UserHttpCommunicationDomain implements Domain {
  public interface Output {
    StateRef<CommunicationStatus> COMMUNICATION_STATUS = new StateRef<>("communication_status", CommunicationStatus.class);
  }

  public enum CommunicationStatus {
    OK, FAILED
  }

  private final HttpRequest httpRequest;
  private final HttpResponse httpResponse;
  private final Whiteboard wb;

  public UserHttpCommunicationDomain(HttpRequest httpRequest, HttpResponse httpResponse, Whiteboard wb) {
    this.httpRequest = httpRequest;
    this.httpResponse = httpResponse;
    this.wb = wb;
  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    jobs.add(new ParseHttpRequestJoba(httpRequest, wb));
    routines.add(new SeqHttpBodyWriteRoutine(httpResponse));
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }

  public BodyPartRef[] allocateBodyParts(int partsCount) {
    final BodyPartRef[] result = new BodyPartRef[partsCount];
    for (int i = 0; i < partsCount; i++) {
      result[i] = new BodyPartRef(partsCount, i);
    }
    return result;
  }

  public Ref<CommunicationStatus, ?> goal() {
    return Output.COMMUNICATION_STATUS;
  }

}
