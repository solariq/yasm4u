package ru.yandex.se.lyadzhin.report;

import com.spbsu.commons.seq.CharSeqBuilder;
import ru.yandex.se.yasm4u.*;
import ru.yandex.se.yasm4u.domains.wb.StateRef;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * User: lyadzhin
 * Date: 04.04.15 10:46
 */
public class UserHttpCommunicationDomain implements Domain {
  public interface Output {
    StateRef<CommunicationStatus> COMMUNICATION_STATUS = new StateRef<>("communication_status", CommunicationStatus.class);

    StateRef<String> YANDEX_UID = new StateRef<>("yandex_uid", String.class);
    StateRef<String> TEXT = new StateRef<>("text", String.class);
  }

  enum CommunicationStatus {
    OK, FAILED
  }

  public static class BodyPartRef extends StateRef<CharSeqBuilder> {
    public final int partsCount;
    public final int partNum;

    public BodyPartRef(int partsCount, int partNum) {
      super("body_part_ref_" + partNum, CharSeqBuilder.class);
      this.partsCount = partsCount;
      this.partNum = partNum;
    }
  }

  private static class PartDoneRef extends StateRef<Boolean> {
    private final int partNum;
    private final int partsCount;

    public PartDoneRef(BodyPartRef bodyPart) {
      super("body_par_ref_" + bodyPart.partNum, Boolean.class);
      this.partNum = bodyPart.partNum;
      this.partsCount = bodyPart.partsCount;
    }
  }

  private final HttpRequest httpRequest;
  private final HttpResponse httpResponse;
  private Whiteboard wb;

  public UserHttpCommunicationDomain(HttpRequest httpRequest, HttpResponse httpResponse, Whiteboard wb) {
    this.httpRequest = httpRequest;
    this.httpResponse = httpResponse;
  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    jobs.add(new ParseHttpRequestJoba(httpRequest, wb));
    routines.add(new SeqHttpBodyWriteRoutine(httpResponse));
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
  }

  public BodyPartRef[] allocateParts(int partsCount) {
    final BodyPartRef[] result = new BodyPartRef[partsCount];
    for (int i = 0; i < partsCount; i++) {
      result[i] = new BodyPartRef(partsCount, i);
    }
    return result;
  }

  public Ref<CommunicationStatus, ?> goal() {
    return Output.COMMUNICATION_STATUS;
  }

  public static class ParseHttpRequestJoba implements Joba {
    private final HttpRequest request;
    private final Whiteboard whiteboard;

    public ParseHttpRequestJoba(HttpRequest request, Whiteboard whiteboard) {
      this.request = request;
      this.whiteboard = whiteboard;
    }

    @Override
    public Ref[] consumes() {
      return new Ref[0];
    }

    @Override
    public Ref[] produces() {
      return new Ref[]{Output.YANDEX_UID, Output.TEXT};
    }

    @Override
    public void run() {
      System.out.println("Parsing HTTP request");
      whiteboard.set(Output.YANDEX_UID.name, "12345");
      whiteboard.set(Output.TEXT.name, "kotiki");
    }
  }

  public static class SeqHttpBodyWriteRoutine implements Routine {
    private final HttpResponse httpResponse;

    public SeqHttpBodyWriteRoutine(HttpResponse httpResponse) {
      this.httpResponse = httpResponse;
    }

    @Override
    public Joba[] buildVariants(Ref[] state, JobExecutorService jes) {
      final List<Joba> result = new ArrayList<>();

      BitSet bitSet = null;
      for (Ref ref : state) {
        if (ref instanceof PartDoneRef) {
          final PartDoneRef partDoneRef = (PartDoneRef) ref;
          if (bitSet == null) {
            bitSet = new BitSet(partDoneRef.partsCount);
            bitSet.set(0, bitSet.length());
          }
          bitSet.set(0, partDoneRef.partNum);
        }
      }

      if (bitSet != null && bitSet.cardinality() == 0) {
        result.add(new FinishCommunicationJoba(httpResponse, jes));
      } else {
        for (Ref ref : state) {
          if (ref instanceof BodyPartRef) {
            final BodyPartRef bodyPartRef = (BodyPartRef) ref;
            if (bodyPartRef.partNum == 0) {
              result.add(new WriteHttpBodyPartJoba(httpResponse, bodyPartRef, jes));
            } else {
              if (bitSet != null && bitSet.get(bodyPartRef.partNum - 1)) {
                result.add(new WriteHttpBodyPartJoba(httpResponse, bodyPartRef, jes));
              }
            }
          }
        }
      }

      return result.toArray(new Joba[result.size()]);
    }

    public class WriteHttpBodyPartJoba implements Joba {
      private final BodyPartRef bodyPartRef;
      private final PartDoneRef partDoneRef;
      private final JobExecutorService jes;

      public WriteHttpBodyPartJoba(HttpResponse httpResponse, BodyPartRef bodyPartRef, JobExecutorService jes) {
        this.bodyPartRef = bodyPartRef;
        this.partDoneRef = new PartDoneRef(bodyPartRef);
        this.jes = jes;
      }

      @Override
      public Ref[] consumes() {
        return new Ref[]{bodyPartRef};
      }

      @Override
      public Ref[] produces() {
        return new Ref[]{partDoneRef};
      }

      @Override
      public void run() {
        // TODO: WTF??
        System.out.println("Writing HTTP body, part = " + bodyPartRef.partNum +
                ", content = " + jes.resolve(bodyPartRef).toString());
        jes.domain(Whiteboard.class).set(partDoneRef.name, true);
      }
    }

    private class FinishCommunicationJoba implements Joba {
      private final JobExecutorService jes;

      public FinishCommunicationJoba(HttpResponse httpResponse, JobExecutorService jes) {
        this.jes = jes;
      }

      @Override
      public Ref[] consumes() {
        return new Ref[0];
      }

      @Override
      public Ref[] produces() {
        return new Ref[] {Output.COMMUNICATION_STATUS};
      }

      @Override
      public void run() {
        System.out.println("Finishing communication");
        jes.domain(Whiteboard.class).set(Output.COMMUNICATION_STATUS.name, CommunicationStatus.OK);
      }
    }
  }
}
