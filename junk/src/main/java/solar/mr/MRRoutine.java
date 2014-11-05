package solar.mr;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.seq.CharSeq;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
abstract class MRRoutine implements Action<CharSeq> {
  protected final MROutput output;

  public MRRoutine(MROutput output) {
    this.output = output;
  }
}
