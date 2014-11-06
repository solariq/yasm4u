package solar.mr;


/**
* User: solar
* Date: 15.10.14
* Time: 11:08
*/
public final class MRTableShard {
  private final String path;
  private final MREnv container;
  private final long metaTS;
  private final boolean exist;
  private final String crc;
  private boolean sorted;

  public MRTableShard(final String path, final MREnv container, final boolean exist, final boolean sorted, final String crc) {
    this(path, container, exist, sorted, crc, System.currentTimeMillis());
  }

  public MRTableShard(final String path, final MREnv container, final boolean exist, final boolean sorted, final String crc,
                      final long ts) {
    this.path = path;
    this.container = container;
    this.exist = exist;
    this.crc = crc;
    this.sorted = sorted;
    metaTS = ts;
  }

  public String path() {
    return path;
  }

  public MREnv container() {
    return container;
  }

  public final long metaTS() {
    return metaTS;
  }

  public boolean isAvailable() {
    return exist;
  }

  public boolean isSorted() {
    return sorted;
  }

  public String crc() {
    if (!isAvailable())
      throw new IllegalStateException("Resource is not available: " + path() + "!");
    return crc;
  }

  public MRTableShard refresh() {
    return container.resolve(path);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MRTableShard)) {
      return false;
    }

    final MRTableShard that = (MRTableShard) o;

    if (!container.name().equals(that.container.name())) {
      return false;
    }
    if (!crc.equals(that.crc)) {
      return false;
    }
    return path.equals(that.path);

  }

  @Override
  public int hashCode() {
    int result = path.hashCode();
    result = 31 * result + container.name().hashCode();
    result = 31 * result + crc.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return container.name() + path;
  }
}
