package ru.yandex.se.lyadzhin.report;

import com.spbsu.commons.seq.CharSeqBuilder;
import ru.yandex.se.yasm4u.*;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;

import java.util.List;

/**
 * User: lyadzhin
 * Date: 04.04.15 11:34
 */
public class ConfigurationDomain implements Domain {
  private final Configuration configuration;
  private final UserHttpCommunicationDomain communication;
  private final Whiteboard wb;

  public ConfigurationDomain(Configuration configuration, UserHttpCommunicationDomain communication, Whiteboard wb) {
    this.configuration = configuration;
    this.communication = communication;
    this.wb = wb;
  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    final UserHttpCommunicationDomain.BodyPartRef[] bodyPartRefs = communication.allocateParts(1);
    jobs.add(new VpResult2BodyPartConverterJoba(bodyPartRefs[0], wb));
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }


  private static class VpResult2BodyPartConverterJoba implements Joba {
    private final UserHttpCommunicationDomain.BodyPartRef bodyPartRef;
    private final Whiteboard whiteboard;

    public VpResult2BodyPartConverterJoba(UserHttpCommunicationDomain.BodyPartRef bodyPartRef, Whiteboard whiteboard) {
      this.bodyPartRef = bodyPartRef;
      this.whiteboard = whiteboard;
    }

    @Override
    public Ref[] consumes() {
      return new Ref[] {ReportBLDomain.Output.VP_RESULT};
    }

    @Override
    public Ref[] produces() {
      return new Ref[] {bodyPartRef} ;
    }

    @Override
    public void run() {
      final CharSeqBuilder charSeqBuilder = new CharSeqBuilder().append("Hello world!");
      whiteboard.set(bodyPartRef.name, charSeqBuilder);
    }
  }
}
