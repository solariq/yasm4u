package functional;

import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeqTools;
import org.jetbrains.annotations.NotNull;
import solar.mr.MREnv;
import solar.mr.MRTableShard;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by inikifor on 02.12.14.
 */
public final class MRTestUtils {

  private MRTestUtils() {}

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

      if (!key.equals(record.key)) return false;
      if (!subkey.equals(record.subkey)) return false;
      if (!value.equals(record.value)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = key.hashCode();
      result = 31 * result + subkey.hashCode();
      result = 31 * result + value.hashCode();
      return result;
    }
  }

}
