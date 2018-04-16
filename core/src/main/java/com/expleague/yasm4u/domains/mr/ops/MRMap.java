package com.expleague.yasm4u.domains.mr.ops;

import com.expleague.yasm4u.domains.mr.MROutput;
import com.expleague.yasm4u.domains.mr.env.MROutputBase;
import com.expleague.yasm4u.domains.mr.ops.impl.MROperation;
import com.expleague.yasm4u.domains.wb.State;
import com.expleague.yasm4u.domains.mr.MRPath;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRMap extends MROperation {
  protected MROutput output;
  public MRMap(final MRPath[] inputTables, final MROutput output, final State state) {
    super(inputTables, output, state);
    this.output = output;
  }

  @Override
  public final void accept(MRRecord rec) {
    map(rec.source, rec.sub, rec.value, rec.key);
  }

  public abstract void map(MRPath table, String sub, CharSequence value, String key);

  @Override
  protected void onEndOfInput() {
    /* should be safe because we're on the same thread */
    ((MROutputBase)output).stop();
  }
}
