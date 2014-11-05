package solar.mr;

import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqComposite;
import com.spbsu.commons.seq.CharSeqTools;

/**
* User: solar
* Date: 23.09.14
* Time: 11:19
*/
public abstract class MRMap extends MRRoutine {
  public MRMap(final MROutput output) {
    super(output);
  }

  @Override
  public void invoke(final CharSeq record) {
    if (record == CharSeq.EMPTY)
      return;
    final CharSequence[] split = CharSeqTools.split(record, '\t');
    if (split.length < 3)
      output.error("Illegal record", "Contains less then 3 fields", record);
    else
      map(split[0].toString(), split[1].toString(), new CharSeqComposite(split, 2, split.length));
  }

  public abstract void map(String key, String sub, CharSequence value);
}
