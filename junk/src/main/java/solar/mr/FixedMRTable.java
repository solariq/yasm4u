package solar.mr;

import com.spbsu.commons.seq.Seq;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 10:42
 */
public class FixedMRTable extends Seq.Stub<String> implements MRTable {
  private final String name;

  public FixedMRTable(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String at(final int i) {
    if (i >= length())
      throw new ArrayIndexOutOfBoundsException();
    return name;
  }

  @Override
  public int length() {
    return 1;
  }

  @Override
  public boolean isImmutable() {
    return true;
  }

  @Override
  public Class<String> elementType() {
    return String.class;
  }
}
