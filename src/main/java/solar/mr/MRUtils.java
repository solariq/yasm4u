package solar.mr;

import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeqTools;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Created by inikifor on 02.12.14.
 */
public final class MRUtils {

  private MRUtils() {}

  public static void writeRecords(MREnv env, String path, Record... records) {
    final StringBuilder sb = new StringBuilder();
    for (Record record: records) {
      sb.append(record + "\n");
    }
    try {
      Reader reader = new InputStreamReader(new ByteArrayInputStream(sb.toString().getBytes("UTF-8")));
      env.write(pathToShard(env, path), reader);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<Record> readRecords(MREnv env, final String path) {
    final List<Record> result = new ArrayList<>();
    env.read(pathToShard(env, path), new Processor<CharSequence>() {
      @Override
      public void process(CharSequence record) {
          result.add(new Record(record));
      }
    });
    return result;
  }

  public static void dropMRTable(MREnv env, String path) {
    env.delete(pathToShard(env, path));
  }

  public static final Record[] createRecordsWithKeys(int kn, String... keys) {
    return createRecordsWithKeys(kn, 0, keys);
  }

  public static final Record[] createRecordsWithKeys(int kn, int start, String... keys) {
    Record[] result = new Record[kn * keys.length];
    int k = 0;
    for(String key: keys) {
      for(int i=start; i<start+kn; i++) {
        result[k * kn + i - start] = new Record(key, "subkey" + i, "value" + i);
      }
      k++;
    }
    return result;
  }

  public static final Record[] createRecords(int start, int n) {
    Record[] result = new Record[n];
    for(int i=start; i<start+n; i++) {
      result[i-start] = new Record("key" + i, "subkey" + i, "value" + i);
    }
    return result;
  }

  public static final Record[] createRecords(int n) {
    return createRecords(0, n);
  }

  /**
   * List tables(paths) that have specified prefix
   *
   * @param env MREnv instance
   * @param pathPrefix pathPrefix
   * @return List of table paths, empty list of no tables with specified prefix
   */
  public static List<String> listTables(MREnv env, String pathPrefix) {
    final MRTableShard[] list = env.list(pathPrefix);
    List<String> result = new ArrayList<>();
    for (MRTableShard mrTableShard : list) {
      result.add(mrTableShard.path());
    }
    return result;
  }

  private static MRTableShard pathToShard(MREnv env, String path) {
    return new MRTableShard(path, env, true, false, "0", 0, 0, 0, System.currentTimeMillis());
  }

  public static final class Record {
    public final String key;
    public final String subkey;
    public final String value;

    public Record(@NotNull CharSequence record) {
      final CharSequence[] split = new CharSequence[3];
      CharSeqTools.trySplit(record, '\t', split);
      if (split.length < 3) {
        throw new RuntimeException("Cannot parse record!");
      } else {
        key = split[0].toString();
        subkey = split[1].toString();
        value = split[2].toString();
      }
    }

    public Record(@NotNull String key, @NotNull String subkey, @NotNull String value) {
      this.key = key;
      this.subkey = subkey;
      this.value = value;
    }

    @Override
    public String toString() {
      return String.format("%s\t%s\t%s", key, subkey, value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Record record = (Record) o;
      return Arrays.equals(new String[] {key, subkey, value}, new String[] {record.key, record.subkey, record.value});
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, subkey, value);
    }
  }

}
