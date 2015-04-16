package ru.yandex.se.lyadzhin.report.http;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;

import java.net.URI;
import java.util.List;

/**
 * User: lyadzhin
 * Date: 04.04.15 10:46
 */
public class UserHttpCommunicationDomain implements Domain {
  public static final Ref<CommunicationStatus,UserHttpCommunicationDomain> REF_COMMUNICATION_STATUS =
          new CommunicationStatusRef();

  public enum CommunicationStatus {
    OK, FAILED
  }

  private final HttpRequest httpRequest;
  private final HttpResponse httpResponse;
  private final Whiteboard wb;

  private CommunicationStatus communicationStatus;
  private TIntObjectMap<CharSequence> responseBodyParts = new TIntObjectHashMap<>();

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

  void setCommunicationStatus(CommunicationStatus communicationStatus) {
    if (this.communicationStatus != null)
      throw new IllegalStateException("communicationStatus is already defined");
    this.communicationStatus = communicationStatus;
  }

  public void addBodyPartContent(int partNum, CharSequence content) {
    if (responseBodyParts.containsKey(partNum))
      throw new IllegalStateException("Part already has content: " + partNum);
    responseBodyParts.put(partNum, content);
  }

  boolean hasPart(int partNum) {
    return responseBodyParts.containsKey(partNum);
  }

  CharSequence getPartContent(int partNum) {
    return responseBodyParts.get(partNum);
  }

  public Ref<CommunicationStatus, UserHttpCommunicationDomain> goal() {
    return REF_COMMUNICATION_STATUS;
  }

  private static class CommunicationStatusRef implements Ref<CommunicationStatus, UserHttpCommunicationDomain> {
    @Override
    public URI toURI() {
      return null;
    }

    @Override
    public Class<CommunicationStatus> type() {
      return CommunicationStatus.class;
    }

    @Override
    public Class<UserHttpCommunicationDomain> domainType() {
      return UserHttpCommunicationDomain.class;
    }

    @Override
    public CommunicationStatus resolve(UserHttpCommunicationDomain controller) {
      return controller.communicationStatus;
    }

    @Override
    public boolean available(UserHttpCommunicationDomain controller) {
      return controller.communicationStatus != null;
    }
  }
}
