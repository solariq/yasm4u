package solar.mr.tables;


import solar.mr.MREnv;
import solar.mr.MRTable;

/**
* User: solar
* Date: 15.10.14
* Time: 11:08
*/
public class MRTableShard {
  private final String path;
  private final MREnv container;
  private final MRTable owner;
  private final long metaTS;
  private final boolean exist;
  private final String crc;

  public MRTableShard(final String path, final MREnv container, final MRTable owner, final boolean exist, final String crc) {
    this(path, container, owner, exist, crc, System.currentTimeMillis());
  }

  public MRTableShard(final String path, final MREnv container, final MRTable owner, final boolean exist, final String crc,
                      final long ts) {
    this.path = path;
    this.container = container;
    this.owner = owner;
    this.exist = exist;
    this.crc = crc;
    metaTS = ts;
  }

  public MRTable owner() {
    return owner;
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
