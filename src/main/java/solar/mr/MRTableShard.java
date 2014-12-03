package solar.mr;


/**
* User: solar
* Date: 15.10.14
* Time: 11:08
*/
public class MRTableShard {
  private final String path;
  private final MREnv container;
  private final long recordsCount;
  private final long metaTS;
  private final boolean exist;
  public final String crc;
  private final boolean sorted;
  private final long length;
  private final long keysCount;

  public MRTableShard(final String path, final MREnv container, final boolean exist, final boolean sorted, final String crc,
                      long length, long keysCount, long recordsCount, final long ts) {
    this.path = path;
    this.container = container;
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
    if (!(o instanceof MRTableShard))
      return false;

    final MRTableShard that = (MRTableShard) o;
    return container().name().equals(that.container().name()) && crc.equals(that.crc) && exist == that.exist && sorted == that.sorted && path().equals(that.path());
  }

  @Override
  public int hashCode() {
    int result = path.hashCode();
    result = 31 * result + container.hashCode();
    result = 31 * result + (exist ? 1 : 0);
    result = 31 * result + crc.hashCode();
    result = 31 * result + (sorted ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return container().name() + path();
  }

  public String path() {
    return path;
  }

  public MREnv container() {
    return container;
  }

  public long recordsCount() {
    return recordsCount;
  }

  public long metaTS() {
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
