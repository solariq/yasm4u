package solar.mr.proc.impl;

import com.spbsu.commons.seq.CharSeqAdapter;
import com.spbsu.commons.seq.CharSeqTools;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * User: solar
 * Date: 29.01.15
 * Time: 17:38
 */
public class MRPath {
  public final Mount mount;
  public final String path;
  public final boolean sorted;
  private boolean directory;

  public MRPath(Mount mount, String path, boolean sorted) {
    this.mount = mount;
    this.path = path;
    this.sorted = sorted;
  }

  public URI resource() {
    try {
      return new URI("mr", "", mount.prefix + path, sorted ? "sorted=true" : "", "");
    } catch (URISyntaxException e) {
      // should never happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return mount.prefix + path + (sorted ? "?sorted=true" : "");
  }

  public MRPath parent() {
    return null;
  }

  public MRPath[] parents() {
    return new MRPath[0];
  }

  public int level() {
    return 0;
  }

  public boolean isRoot() {
    return false;
  }

  public boolean isDirectory() {
    return directory;
  }

  public static MRPath create(MRPath parent, String path) {
    return null;
  }

  public URI toURI() {
    return null;
  }

  public static MRPath createFromURI(String uri) {
    return null;
  }

  public enum Mount {
    ROOT("/"),
    TEMP("/tmp/"),
    HOME("/home/"),
    ;

    String prefix;
    Mount(String prefix){
      this.prefix = prefix;
    }
  }

  /**
   * MRPath is universal path for any MREnv. It consists of 3 well known parts of URI: path, query and fragment, incorporated into this entity.
   * To support differences between environments, 3 special mounts exists: root (/), home (/home/) and temporary directory (/tmp/). Appropriate prefixes are
   * treated differently depending on the environment.
   * Query parameters:
   * sorted --- shows if the required table must be sorted, this parameter appended automatically to any input of reduce operation
   *
   * @param source this parameter contains path+query+fragment parts of URI.
   */
  public static MRPath create(String source) {
    int attrsStart = source.indexOf("?") + 1;
    boolean sorted = false;
    final CharSequence[] key2value = new CharSequence[2];
    while (attrsStart > 0) {
      final int attrsStartNext = source.indexOf("&", attrsStart);
      CharSeqTools.split(
              new CharSeqAdapter(source, attrsStart, attrsStartNext > 0 ? attrsStartNext : source.length()),
              '=', key2value);
      if (key2value[0].equals("sorted")) {
        sorted = Boolean.parseBoolean(key2value[1].toString());
      }
      else throw new IllegalArgumentException("Unknown attribute " + key2value[0] + " in resource " + source);
      attrsStart = attrsStartNext + 1;
    }

    Mount mount = null;
    for (final Mount mnt : Mount.values()) {
      if (source.startsWith(mnt.prefix))
        mount = mnt;
    }

    if (mount == null)
      throw new IllegalArgumentException("Unknown mount: " + source);
    final String path = source.substring(mount.prefix.length());

    return new MRPath(mount, path, sorted);
  }
}
