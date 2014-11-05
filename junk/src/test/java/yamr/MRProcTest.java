package yamr;

import java.util.Date;
import java.util.Iterator;
import java.util.Random;


import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import solar.mr.MREnv;
import solar.mr.MROutput;
import solar.mr.env.LocalMREnv;
import solar.mr.env.ProcessRunner;
import solar.mr.env.SSHProcessRunner;
import solar.mr.env.YaMREnv;
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

  @MRProcessClass(goal = "var:result")
  public static class FailAtRandom {
    private final MRState state;
    private final Random rng = new FastRandom();
    int index = 0;

    public FailAtRandom(MRState state) {
      this.state = state;
    }

    @MRMapMethod(input = {"mr:///user_sessions/{var:date,date,yyyyMMdd}", "var:delay"}, output = "temp:mr:///dev-null")
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      if (index > state.<Integer>get("var:delay"))
        throw new RuntimeException("Preved s clustera");
      index++;
    }


    @MRRead(input = "temp:mr:///dev-null", output = "var:result")
    public int poh(Iterator<CharSequence> line) {
      return 0;
    }
  }


  @Test
  public void testProcCreate() {
    final ProcessRunner runner = new SSHProcessRunner("dodola", "/Berkanavt/mapreduce/bin/mapreduce-dev");
    final MREnv env = new YaMREnv(runner, "mobilesearch", "cedar:8013");
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(SAPPCounter.class, env);
    mrProcess.wb().set("var:date", new Date(2014-1900, 8, 1));
    int count = mrProcess.<Integer>result();
    Assert.assertEquals(2611709, count);
    mrProcess.wb().wipe();
  }

  @Test
  public void testException() {
    final ProcessRunner runner = new SSHProcessRunner("dodola", "/Berkanavt/mapreduce/bin/mapreduce-dev");
    final MREnv env = new YaMREnv(runner, "mobilesearch", "cedar:8013");
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(FailAtRandom.class, env);
    mrProcess.wb().wipe();
    mrProcess.wb().set("var:date", new Date(2014-1900, 8, 1));
    mrProcess.wb().set("var:delay", 10000);
    int count = mrProcess.<Integer>result();
    Assert.assertEquals(0, count);
    mrProcess.wb().wipe();
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
