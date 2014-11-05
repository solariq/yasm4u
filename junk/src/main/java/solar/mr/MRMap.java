package solar.mr;

import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqComposite;
import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.proc.MRState;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRMap extends MRRoutine {
  public MRMap(final String[] inputTables, final MROutput output, final MRState state) {
    super(inputTables, output, state);
  }

  @Override
  public final void invoke(Record rec) {
    map(rec.key, rec.sub, rec.value);
  }

  public abstract void map(String key, String sub, CharSequence value);
}
