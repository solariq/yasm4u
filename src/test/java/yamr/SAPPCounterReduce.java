package yamr;

import java.util.Iterator;


import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;
import ru.yandex.se.yasm4u.domains.mr.ops.MRReduce;

/**
* User: solar
* Date: 26.09.14
* Time: 1:56
*/
public class SAPPCounterReduce extends MRReduce {
  public SAPPCounterReduce(final MRPath[] inputTables, final MROutput output, final State state) {
    super(inputTables, output, state);
  }

  @Override
  public void reduce(final String key, final Iterator<MRRecord> reduce) {
    int count = 0;
    while (reduce.hasNext()) {
      reduce.next();
      count++;
    }
    output.add(key, "1", "" + count);
  }
}
