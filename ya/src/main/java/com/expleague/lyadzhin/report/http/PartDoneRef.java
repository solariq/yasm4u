package com.expleague.lyadzhin.report.http;

import com.expleague.yasm4u.domains.wb.StateRef;

/**
* User: lyadzhin
* Date: 08.04.15 19:14
*/
class PartDoneRef extends StateRef<Boolean> {
  private final int partNum;
  private final int partsCount;

  public PartDoneRef(BodyPartRef bodyPart) {
    super("body_par_ref_" + bodyPart.partNum, Boolean.class);
    this.partNum = bodyPart.partNum;
    this.partsCount = bodyPart.partsCount;
  }

  public int getPartNum() {
    return partNum;
  }

  public int getPartsCount() {
    return partsCount;
  }
}
