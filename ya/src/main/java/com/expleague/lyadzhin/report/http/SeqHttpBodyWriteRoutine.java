package com.expleague.lyadzhin.report.http;

import com.expleague.yasm4u.JobExecutorService;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.Routine;
import com.expleague.yasm4u.domains.wb.Whiteboard;

import java.util.ArrayList;
import java.util.BitSet;
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

    BitSet bitSet = null;
    for (Ref ref : state) {
      if (ref instanceof PartDoneRef) {
        final PartDoneRef partDoneRef = (PartDoneRef) ref;
        if (bitSet == null) {
          bitSet = new BitSet(partDoneRef.getPartsCount());
          bitSet.set(0, bitSet.length());
        }
        if (jes.available(ref))
          bitSet.set(0, partDoneRef.getPartNum());
      }
    }

    if (bitSet != null && bitSet.cardinality() == 0) {
      result.add(new FinishCommunicationJoba(httpResponse, jes.domain(Whiteboard.class)));
    } else {
      for (Ref ref : state) {
        if (ref instanceof BodyPartRef) {
          final BodyPartRef bodyPartRef = (BodyPartRef) ref;
          if (bodyPartRef.partNum == 0) {
            result.add(new WriteHttpBodyPartJoba(httpResponse, bodyPartRef, jes.domain(Whiteboard.class)));
          } else {
            if (bitSet != null && bitSet.get(bodyPartRef.partNum - 1)) {
              result.add(new WriteHttpBodyPartJoba(httpResponse, bodyPartRef, jes.domain(Whiteboard.class)));
            }
          }
        }
      }
    }

    return result.toArray(new Joba[result.size()]);
  }

}
