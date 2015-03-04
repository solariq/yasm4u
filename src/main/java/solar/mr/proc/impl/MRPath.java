package solar.mr.proc.impl;

import com.spbsu.commons.seq.CharSeqAdapter;
import com.spbsu.commons.seq.CharSeqTools;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

/**
 * User: solar
 * Date: 29.01.15
 * Time: 17:38
 */
public class MRPath implements Serializable {
  public final Mount mount;
  public final String path;
  public final boolean sorted;
  private final static EnumSet<Mount> mounts = EnumSet.allOf(Mount.class);

  public MRPath(Mount mount, String path, boolean sorted) {
    this.mount = mount;
    this.path = path;
    this.sorted = sorted;
  }

  public URI resource() {
    try {
      if (!isDirectory())
        return new URI("mr", "", mount.prefix + path, "sorted=" + Boolean.toString(sorted), "");
      else
        return new URI("mr://" + mount.prefix + path);
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
    if (isRoot())
      return null;
    if ("".equals(path))
      return new MRPath(Mount.ROOT, "", false);
    return new MRPath(mount, path.substring(0, path.substring(0, path.length() - 1).lastIndexOf('/') + 1), false);
  }

  public MRPath[] parents() {
    final MRPath[] parents = new MRPath[level()];
    MRPath parent = parent();
    for(int i = 0; i < parents.length; i++) {
      parents[i] = parent;
      parent = parent.parent();
    }
    return parents;
  }

  public int level() {
    return (mount == Mount.ROOT ? 0 : 1) + CharSeqTools.count(path, 0, path.length() - 1, '/');
  }

  public boolean isRoot() {
    return "".equals(path) && mount == Mount.ROOT;
  }
  
  public boolean isMountRoot() {
    return "".equals(path) && mounts.contains(mount);
  }

  public boolean isDirectory() {
    return isMountRoot() || path.endsWith("/");
  }

  public String absolutePath() {
    return mount.prefix + path;
  }

  public String tableName() {
    if (isDirectory())
      throw new RuntimeException("this is directory");
    return path.substring(path.lastIndexOf("/") + 1);
  }

  public enum Mount {
    ROOT("/"),
    TEMP("/tmp/"),
    HOME("/home/"),
    LOG("/log/");

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
    if (source.contains("//"))
      throw new RuntimeException("//");

    final int attrsStart = source.indexOf("?") + 1;
    boolean sorted = false;
    {
      int nextAttrStart = attrsStart;
      final CharSequence[] key2value = new CharSequence[2];
      while (nextAttrStart > 0) {
        final int attrsStartNext = source.indexOf("&", nextAttrStart);
        CharSeqTools.split(
                new CharSeqAdapter(source, nextAttrStart, attrsStartNext > 0 ? attrsStartNext : source.length()),
                '=', key2value);
        if (key2value[0].equals("sorted")) {
          sorted = Boolean.parseBoolean(key2value[1].toString());
        }
        else throw new IllegalArgumentException("Unknown attribute " + key2value[0] + " in resource " + source);
        nextAttrStart = attrsStartNext + 1;
      }
    }

    Mount mount = null;
    for (final Mount mnt : Mount.values()) {
      if (source.startsWith(mnt.prefix))
        mount = mnt;
    }

    if (mount == null) {
      mount=Mount.ROOT; //throw new IllegalArgumentException("Unknown mount: " + source);
    }
    String path = source;
    if (attrsStart > 0)
      path = path.substring(0, attrsStart - 1);
    path = path.substring(mount.prefix.length());

    return new MRPath(mount, path, sorted);
  }

  public static MRPath create(MRPath parent, String path) {
    if (path.contains("//"))
      throw new RuntimeException("//");
    
    if (path.startsWith("/"))
      throw new RuntimeException("path started with /");

    if (!parent.isDirectory())
      throw new IllegalArgumentException("Parent must be directory but [" + parent + "] is not.");
    return new MRPath(parent.mount, parent.path + path, false);
  }

  public static MRPath createFromURI(String uriS) {
    try {
      /*if (uriS.equals("//"))
        throw new RuntimeException("//");*/
      
      final URI uri = new URI(uriS);
      if ("mr".equals(uri.getScheme())) {
        if (uri.getPath().contains("//"))
          throw new RuntimeException("//");
        return create(uri.getPath() + (uri.getQuery() != null ? "?" + uri.getQuery() : ""));
      }
      else throw new IllegalArgumentException("Unsupported protocol: " + uri.getScheme() + " in URI: [" + uriS + "]");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MRPath)) return false;

    MRPath mrPath = (MRPath) o;

    if (sorted != mrPath.sorted) return false;
    if (mount != mrPath.mount) return false;
    return path.equals(mrPath.path);
  }

  @Override
  public int hashCode() {
    int result = mount.hashCode();
    result = 31 * result + path.hashCode();
    result = 31 * result + (sorted ? 1 : 0);
    return result;
  }
}
