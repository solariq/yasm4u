package yamr;

import java.util.Iterator;


import com.spbsu.commons.util.Pair;
import solar.mr.MROutput;
import solar.mr.MRReduce;

/**
* User: solar
* Date: 26.09.14
* Time: 1:56
*/
public class SAPPCounterReduce extends MRReduce {
  public SAPPCounterReduce(final MROutput output) {
    super(output);
  }

  @Override
  public void reduce(final String key, final Iterator<Pair<String, CharSequence>> reduce) {
    int count = 0;
    while (reduce.hasNext()) {
      reduce.next();
      count++;
    }
    output.add("SAPP", "", "" + count);
  }
}
