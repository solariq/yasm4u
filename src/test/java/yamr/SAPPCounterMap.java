package yamr;


import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.routines.MRMap;
import solar.mr.MROutput;
import solar.mr.proc.MRState;

/**
* User: solar
* Date: 25.09.14
* Time: 23:56
*/
public class SAPPCounterMap extends MRMap {
  public SAPPCounterMap(final String[] inputTables, final MROutput output, final MRState state) {
    super(inputTables, output, state);
  }

  @Override
  public void map(final String key, final String sub, final CharSequence value) {
    final CharSequence[] parts = CharSeqTools.split(value, '\t');
    for (int i = 0; i < parts.length; i++) {
      if (CharSeqTools.startsWith(parts[i], "reqid=")) {
        final CharSequence suffix = parts[i].subSequence(parts[i].toString().lastIndexOf('-') + 1, parts[i].length());
        if (CharSeqTools.isAlpha(suffix)) {
          output.add(suffix.toString(), "1", "1");
        }
      }
    }
  }
}
