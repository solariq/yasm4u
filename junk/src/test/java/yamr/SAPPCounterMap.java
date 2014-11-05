package yamr;

import java.util.regex.Matcher;


import com.spbsu.commons.seq.CharSeqAdapter;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.seq.regexp.SimpleRegExp;
import solar.mr.MRMap;
import solar.mr.MROutput;

/**
* User: solar
* Date: 25.09.14
* Time: 23:56
*/
public class SAPPCounterMap extends MRMap {
  public SAPPCounterMap(final MROutput output) {
    super(output);
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
