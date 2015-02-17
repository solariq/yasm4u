package solar.mr;


/**
* User: solar
* Date: 15.10.14
* Time: 11:08
*/
public class MRTableState {
  public final String crc;
  private final String path;
  private final long recordsCount;
  private final long metaTS;
  private final boolean exist;
  private final boolean sorted;
  private final long length;
  private final long keysCount;

  public MRTableState(final String path, final boolean exist, final boolean sorted, final String crc,
                      long length, long keysCount, long recordsCount, final long ts) {
    this.path = path;
    this.exist = exist;
    this.crc = crc;
    this.sorted = sorted;
    this.length = length;
    this.keysCount = keysCount;
    this.recordsCount = recordsCount;
    this.metaTS = ts;
  }

  public String crc() {
    if (!isAvailable())
      throw new IllegalStateException("Resource is not available: " + path() + "!");
    return crc;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof MRTableState))
      return false;

    final MRTableState that = (MRTableState) o;
    return crc.equals(that.crc) && exist == that.exist && sorted == that.sorted && path().equals(that.path());
  }

  @Override
  public int hashCode() {
    int result = path.hashCode();
    result = 31 * result + (exist ? 1 : 0);
    result = 31 * result + crc.hashCode();
    result = 31 * result + (sorted ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(getClass().getName());
    builder.append("[").append("path: ").append(path).append(", ");
    builder.append("exists: ").append(exist).append(", ");
    builder.append("sorted: ").append(sorted).append(", ");
    builder.append("length: ").append(length).append(", ");
    builder.append("keysCount: ").append(keysCount).append(", ");
    builder.append("records count: ").append(recordsCount).append(", ");
    builder.append("crc: ").append(crc).append(", ");
    builder.append("timestamp: ").append(metaTS).append("]");
    return builder.toString();
  }

  public String path() {
    return path;
  }

  public long recordsCount() {
    return recordsCount;
  }

  public long snapshotTime() {
    return metaTS;
  }

  public boolean isAvailable() {
    return exist;
  }

  public boolean isSorted() {
    return sorted;
  }

  public long length() {
    return length;
  }

  public long keysCount() {
    return keysCount;
  }
}
