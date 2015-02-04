package yamr;

import java.util.Iterator;


import solar.mr.MROutput;
import solar.mr.proc.State;
import solar.mr.proc.impl.MRPath;
import solar.mr.routines.MRRecord;
import solar.mr.routines.MRReduce;

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
