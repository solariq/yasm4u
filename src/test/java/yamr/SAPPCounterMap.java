package yamr;


import com.spbsu.commons.seq.CharSeqTools;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.ops.MRMap;
import ru.yandex.se.yasm4u.domains.mr.MROutput;

/**
* User: solar
* Date: 25.09.14
* Time: 23:56
*/
public class SAPPCounterMap extends MRMap {
  public SAPPCounterMap(final MRPath[] inputTables, final MROutput output, final State state) {
    super(inputTables, output, state);
  }

  @Override
  public void map(MRPath table, final String sub, final CharSequence value, final String key) {
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
