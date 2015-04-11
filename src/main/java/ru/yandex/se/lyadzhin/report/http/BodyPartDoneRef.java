package ru.yandex.se.lyadzhin.report.http;

import ru.yandex.se.yasm4u.Ref;

import java.net.URI;

/**
* User: lyadzhin
* Date: 08.04.15 19:14
*/
class BodyPartDoneRef implements Ref<Void,UserHttpCommunicationDomain> {
  public final int partNum;

  public BodyPartDoneRef(int partNum) {
    this.partNum = partNum;
  }

  @Override
  public URI toURI() {
    return null;
  }

  @Override
  public Class<Void> type() {
    return Void.class;
  }

  @Override
  public Class<UserHttpCommunicationDomain> domainType() {
    return UserHttpCommunicationDomain.class;
  }

  @Override
  public Void resolve(UserHttpCommunicationDomain controller) {
    return null;
  }

  @Override
  public boolean available(UserHttpCommunicationDomain controller) {
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    BodyPartDoneRef that = (BodyPartDoneRef) o;
    return partNum == that.partNum;
  }

  @Override
  public int hashCode() {
    return partNum;
  }
}
