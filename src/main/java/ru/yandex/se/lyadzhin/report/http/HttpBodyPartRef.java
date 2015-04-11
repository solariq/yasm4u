package ru.yandex.se.lyadzhin.report.http;

import ru.yandex.se.yasm4u.Ref;

import java.net.URI;

/**
* User: lyadzhin
* Date: 08.04.15 19:13
*/
public class HttpBodyPartRef implements Ref<CharSequence,UserHttpCommunicationDomain> {
  public final int partNum;

  public HttpBodyPartRef(int partNum) {
    this.partNum = partNum;
  }

  @Override
  public URI toURI() {
    return null;
  }

  @Override
  public Class<CharSequence> type() {
    return CharSequence.class;
  }

  @Override
  public Class<UserHttpCommunicationDomain> domainType() {
    return UserHttpCommunicationDomain.class;
  }

  @Override
  public CharSequence resolve(UserHttpCommunicationDomain controller) {
    return controller.getPartContent(partNum);
  }

  @Override
  public boolean available(UserHttpCommunicationDomain controller) {
    return controller.getPartContent(partNum) != null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    HttpBodyPartRef that = (HttpBodyPartRef) o;
    return partNum == that.partNum;

  }

  @Override
  public int hashCode() {
    return partNum;
  }
}
