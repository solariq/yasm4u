package solar.mr;

import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqReader;
import solar.mr.proc.impl.MRPath;
import solar.mr.routines.MRRecord;

import java.io.*;
import java.util.*;

/**
 * Created by inikifor on 02.12.14.
 */
public final class MRTestUtils {

  private MRTestUtils() {}

  public static void writeRecords(MREnv env, String uri, MRRecord... records) {
    final StringBuilder sb = new StringBuilder();
    for (MRRecord record: records) {
      sb.append(record.toString()).append("\n");
    }
    try (Reader reader = new InputStreamReader(new ByteArrayInputStream(sb.toString().getBytes("UTF-8")))){
      env.write(MRPath.createFromURI(uri), reader);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<MRRecord> readRecords(MREnv env, final String path) {
    final List<MRRecord> result = new ArrayList<>();
    env.read(MRPath.createFromURI(path), new Processor<MRRecord>() {
      @Override
      public void process(MRRecord record) {
          result.add(record);
      }
    });
    return result;
  }

  public static List<MRRecord> readRecords(MREnv env, final MRPath path) {
    final List<MRRecord> result = new ArrayList<>();
    env.read(path, new Processor<MRRecord>() {
      @Override
      public void process(MRRecord record) {
          result.add(record);
      }
    });
    return result;
  }

  public static MRRecord[] createRecordsWithKeys(int kn, String... keys) {
    return createRecordsWithKeys(kn, 0, keys);
  }

  public static MRRecord[] createRecordsWithKeys(int kn, int start, String... keys) {
    MRRecord[] result = new MRRecord[kn * keys.length];
    int k = 0;
    for(String key: keys) {
      for(int i=start; i<start+kn; i++) {
        result[k * kn + i - start] = new MRRecord(MRPath.create("/dev/null"), key, "subkey" + i, "value" + i);
      }
      k++;
    }
    return result;
  }

  public static MRRecord[] createRecords(int start, int n) {
    MRRecord[] result = new MRRecord[n];
    for(int i=start; i<start+n; i++) {
      result[i-start] = new MRRecord(MRPath.create("/dev/null"), "key" + i, "subkey" + i, "value" + i);
    }
    return result;
  }

  public static MRRecord[] createRecords(int n) {
    return createRecords(0, n);
  }

  public static void writeRecords(MREnv env, MRRecord... mrRecord) {
    final Map<MRPath, CharSeqBuilder> toWrite = new HashMap<>();
    for (MRRecord record : mrRecord) {
      CharSeqBuilder builder = toWrite.get(record.source);
      if (builder == null)
        toWrite.put(record.source, builder = new CharSeqBuilder());
      builder.append(record.toString()).append("\n");
    }
    for (Map.Entry<MRPath, CharSeqBuilder> entry : toWrite.entrySet()) {
      env.write(entry.getKey(), new CharSeqReader(entry.getValue().build()));
    }
  }

  public static void dropMRTable(final MREnv env, String url){
    for (final MRPath p: env.list(MRPath.createFromURI(url))) {
      env.delete(p);
    }
  }

  public static List<MRPath> listTables(final MREnv env, final MRPath path){
    final ArrayList<MRPath> result = new ArrayList<>();
    for (final MRPath p:env.list(path)){
      result.add(p);
    }
    return result;
  }
}
