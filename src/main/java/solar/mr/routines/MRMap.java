package solar.mr.routines;

import solar.mr.MROutput;
import solar.mr.MRRoutine;
import solar.mr.proc.State;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRMap extends MRRoutine {
  public MRMap(final String[] inputTables, final MROutput output, final State state) {
    super(inputTables, output, state);
  }

  @Override
  public final void invoke(MRRecord rec) {
    map(rec.key, rec.sub, rec.value);
  }

  public abstract void map(String key, String sub, CharSequence value);
}
