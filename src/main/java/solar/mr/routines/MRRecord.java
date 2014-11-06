package solar.mr.routines;

/**
* User: solar
* Date: 05.11.14
* Time: 15:58
*/
public class MRRecord {
  public String source;
  public String key;
  public String sub;
  public CharSequence value;

  public MRRecord(final String source, final String key, final String sub, final CharSequence value) {
    this.source = source;
    this.key = key;
    this.sub = sub;
    this.value = value;
  }

  @Override
  public String toString() {
    return key + "\t" + sub + "\t" + value.toString();
  }
}
