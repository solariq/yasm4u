package ru.yandex.se.yasm4u.domains.mr.ops;

import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MROperation;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.mr.MRPath;

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
  public final void process(MRRecord rec) {
    map(rec.source, rec.sub, rec.value, rec.key);
  }

  public abstract void map(MRPath table, String sub, CharSequence value, String key);
}
