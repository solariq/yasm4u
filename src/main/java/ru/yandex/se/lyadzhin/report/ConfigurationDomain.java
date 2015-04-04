package ru.yandex.se.lyadzhin.report;

import com.spbsu.commons.seq.CharSeqBuilder;
import ru.yandex.se.yasm4u.*;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;

/**
 * User: lyadzhin
 * Date: 04.04.15 11:34
 */
public class ConfigurationDomain implements Domain {
  public ConfigurationDomain(Configuration configuration) {
  }

  @Override
  public void init(final JobExecutorService jes) {
    final UserHttpCommunicationDomain.BodyPartRef[] bodyPartRefs =
            jes.domain(UserHttpCommunicationDomain.class).allocateParts(1);
    jes.addJoba(new VpResult2BodyPartConverterJoba(bodyPartRefs[0], jes.domain(Whiteboard.class)));
  }

  private static class VpResult2BodyPartConverterJoba implements Joba {
    private final UserHttpCommunicationDomain.BodyPartRef bodyPartRef;
    private final Whiteboard whiteboard;

    public VpResult2BodyPartConverterJoba(UserHttpCommunicationDomain.BodyPartRef bodyPartRef, Whiteboard whiteboard) {
      this.bodyPartRef = bodyPartRef;
      this.whiteboard = whiteboard;
    }

    @Override
    public Ref<?>[] consumes() {
      return new Ref<?>[] {ReportBLDomain.Output.VP_RESULT};
    }

    @Override
    public Ref<?>[] produces() {
      return new Ref<?>[] {bodyPartRef} ;
    }

    @Override
    public void run() {
      final CharSeqBuilder charSeqBuilder = new CharSeqBuilder().append("Hello world!");
      whiteboard.set(bodyPartRef.name, charSeqBuilder);
    }
  }
}
