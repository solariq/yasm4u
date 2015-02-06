package solar.mr.routines;

import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.proc.impl.MRPath;

/**
* User: solar
* Date: 05.11.14
* Time: 15:58
*/
public class MRRecord {
  public static final MRRecord EMPTY = new MRRecord(MRPath.create("/dev/null"), "", "", "");
  public final MRPath source;
  public final String key;
  public final String sub;
  public final CharSequence value;

  public MRRecord(final MRPath source, final String key, final String sub, final CharSequence value) {
    this.source = source;
    this.key = key;
    this.sub = sub;
    this.value = value;
  }

  @Override
  public String toString() {
    return key + "\t" + sub + "\t" + value.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MRRecord)) return false;

    final MRRecord mrRecord = (MRRecord) o;
    return key.equals(mrRecord.key) && sub.equals(mrRecord.sub) && CharSeqTools.equals(value, mrRecord.value);
  }

  @Override
  public int hashCode() {
    int result = key.hashCode();
    result = 31 * result + sub.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }
}
