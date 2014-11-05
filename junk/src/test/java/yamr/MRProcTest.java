package yamr;

import java.io.File;
import java.util.Date;
import java.util.Iterator;


import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import solar.mr.MREnv;
import solar.mr.MROutput;
import solar.mr.env.LocalMREnv;
import solar.mr.MRTable;
import solar.mr.env.RemoteYaMREnvironment;
import solar.mr.proc.MRState;
import solar.mr.proc.MRWhiteboard;
import solar.mr.proc.impl.AnnotatedMRProcess;
import solar.mr.proc.impl.MRWhiteboardImpl;
import solar.mr.proc.tags.MRMapMethod;
import solar.mr.proc.tags.MRProcessClass;
import solar.mr.proc.tags.MRRead;
import solar.mr.proc.tags.MRReduceMethod;
import solar.mr.tables.MRTableShard;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:35
 */
public class MRProcTest {
  @MRProcessClass(goal = "var:result")
  public static class SAPPCounter {
    private final MRState state;

    public SAPPCounter(MRState state) {
      this.state = state;
    }

    @MRMapMethod(input = "mr:///user_sessions/{var:date,date,yyyyMMdd}", output = {"temp:mr:///counter-map"})
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      final CharSequence[] parts = CharSeqTools.split(value, '\t');
      for (int i = 0; i < parts.length; i++) {
        if (CharSeqTools.startsWith(parts[i], "reqid=")) {
          final CharSequence suffix = parts[i].subSequence(parts[i].toString().lastIndexOf('-') + 1, parts[i].length());
          if (CharSeqTools.isAlpha(suffix)) {
            output.add(suffix.toString(), "1", "1");
          }
        }
      }
    }

    @MRReduceMethod(input = "temp:mr:///counter-map", output = {"temp:mr:///counter-result"})
    public void reduce(final String key, final Iterator<Pair<String, CharSequence>> reduce, MROutput output) {
      int count = 0;
      while (reduce.hasNext()) {
        reduce.next();
        count++;
      }
      output.add(key, "1", "" + count);
    }

    @MRRead(input = "temp:mr:///counter-result", output = "var:result")
    public int count(Iterator<CharSequence> line) {
      int result = 0;
      while (line.hasNext()) {
        final CharSequence[] split = CharSeqTools.split(line.next(), "\t");
        if (split[0].equals("SAPP"))
          result = CharSeqTools.parseInt(split[2]);
      }
      return result;
    }
  }

  @Test
  public void testProcCreate() {
    final MREnv env = new RemoteYaMREnvironment("dodola", "/Berkanavt/mapreduce/bin/mapreduce-dev", "cedar:8013", "mobilesearch");
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(SAPPCounter.class, env);
    mrProcess.wb().set("var:date", new Date(2014-1900, 8, 1));
    int count = mrProcess.<Integer>result();
    mrProcess.wb().clear();
    Assert.assertEquals(246573, count);
  }

  @Test
  public void testResolve() {
    final MREnv env = LocalMREnv.createTemp();
    final MRWhiteboard wb = new MRWhiteboardImpl(env, "proc", "none");
    wb.set("xxx", "yyy");
    final String resolveString = wb.resolve("{var:xxx}");
    final MRTableShard resolveTable = wb.resolve("mr://xxx");

    Assert.assertEquals("yyy", resolveString);
  }
}
