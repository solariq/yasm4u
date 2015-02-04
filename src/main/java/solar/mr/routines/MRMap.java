package solar.mr.routines;

import solar.mr.MROutput;
import solar.mr.MRRoutine;
import solar.mr.proc.State;
import solar.mr.proc.impl.MRPath;

import java.util.concurrent.*;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRMap extends MRRoutine {
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
