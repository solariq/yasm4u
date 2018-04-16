package com.expleague.lyadzhin.report.sources;

import com.expleague.yasm4u.Domain;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.Routine;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * User: lyadzhin
 * Date: 04.04.15 12:46
 */
public class SourceCommunicationDomain implements Domain {
  private final Set<SourceResponse> responses = new HashSet<>();

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    routines.add(new PublishSourceRequestExecutorsRoutine(this));
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }

  public void request(String sourceKey) {

  }

  void addResponse(SourceResponse response) {
    responses.add(response);
  }

  boolean hasResponse(SourceResponse response) {
    return responses.contains(response);
  }

  public static enum RequestStatus {
    OK, FAILED
  }

}
