package yamr;

import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
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
import solar.mr.MRTableShard;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:35
 */
@SuppressWarnings("deprecation")
public class MRProcTest {

  private final static String TEST_SERVER_PROXY = "batista";
  private final static String TEST_MR_USER = "mobilesearch";

  @SuppressWarnings("UnusedDeclaration")
  @MRProcessClass(goal = "var:result")
  public static class SAPPCounter {
    public SAPPCounter(MRState state) {}

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

  @SuppressWarnings("UnusedDeclaration")
  @MRProcessClass(goal = "var:result")
  public static class FailAtRandomMap {
    private final MRState state;
    private final Random rng = new FastRandom();
    int index = 0;

    public FailAtRandomMap(MRState state) {
      this.state = state;
    }

    @MRMapMethod(input = {"mr:///user_sessions/{var:date,date,yyyyMMdd}", "var:delay"}, output = "temp:mr:///dev-null")
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      //noinspection ConstantConditions
      if (index > state.<Integer>get("var:delay"))
        throw new RuntimeException("Preved s clustera");
      index++;
    }


    @MRRead(input = "temp:mr:///dev-null", output = "var:result")
    public int poh(Iterator<CharSequence> line) {
      return 0;
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  @MRProcessClass(goal = "var:result")
  public static class FailAtRandomReduce {
    private final MRState state;
    private final Random rng = new FastRandom();
    int index = 0;

    public FailAtRandomReduce(MRState state) {
      this.state = state;
    }

    @SuppressWarnings("ConstantConditions")
    @MRReduceMethod(input = {"mr:///user_sessions/{var:date,date,yyyyMMdd}", "var:delay"}, output = "temp:mr:///dev-null")
    public void reduce(final String key, final Iterator<Pair<String, CharSequence>> reduce, MROutput output) {
      while (reduce.hasNext()) {
        reduce.next();
        if (index > state.<Integer>get("var:delay"))
          throw new RuntimeException("Preved s clustera");
        index++;
      }
    }


    @MRRead(input = "temp:mr:///dev-null", output = "var:result")
    public int poh(Iterator<CharSequence> line) {
      return 0;
    }
  }


  @Test
  public void testProcCreate() {
    final ProcessRunner runner = new SSHProcessRunner(TEST_SERVER_PROXY, "/Berkanavt/mapreduce/bin/mapreduce-dev");
    final MREnv env = new YaMREnv(runner, TEST_MR_USER, "cedar:8013");
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(SAPPCounter.class, env);
    mrProcess.wb().wipe();
    mrProcess.wb().set("var:date", new Date(2014-1900, 8, 1));
    int count = mrProcess.<Integer>result();
    Assert.assertEquals(2611709, count);
  }

  @Test
  public void testExceptionMap() {
    final ProcessRunner runner = new SSHProcessRunner(TEST_SERVER_PROXY, "/Berkanavt/mapreduce/bin/mapreduce-dev");
    final MREnv env = new YaMREnv(runner, TEST_MR_USER, "cedar:8013");
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(FailAtRandomReduce.class, env);
    mrProcess.wb().wipe();
    mrProcess.wb().set("var:date", new Date(2014-1900, 8, 1));
    mrProcess.wb().set("var:delay", 10000);
    int count = mrProcess.<Integer>result();
    Assert.assertEquals(0, count);
    mrProcess.wb().wipe();
  }
  @Test
  public void testExceptionReduce() {
    final ProcessRunner runner = new SSHProcessRunner(TEST_SERVER_PROXY, "/Berkanavt/mapreduce/bin/mapreduce-dev");
    final MREnv env = new YaMREnv(runner, TEST_MR_USER, "cedar:8013");
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(FailAtRandomReduce.class, env);
    mrProcess.wb().wipe();
    mrProcess.wb().set("var:date", new Date(2014-1900, 8, 1));
    mrProcess.wb().set("var:delay", 1000);
    int count = mrProcess.<Integer>result();
    Assert.assertEquals(0, count);
    mrProcess.wb().wipe();
  }

  @Test
  public void testResolve() {
    final MREnv env = LocalMREnv.createTemp();
    final MRWhiteboard wb = new MRWhiteboardImpl(env, "proc", "none");
    wb.set("var:xxx", "yyy");
    final String resolveString = wb.get("{var:xxx}");
    @SuppressWarnings("UnusedDeclaration")
    final MRTableShard resolveTable = wb.get("mr://xxx");

    Assert.assertEquals("yyy", resolveString);

    wb.set("var:xx1", new Date(2014-1900,7,1));
    Assert.assertEquals("20140801", wb.get("{var:xx1,date,yyyyMMdd}"));
    @SuppressWarnings("UnusedAssignment")
    String path = wb.<MRTableShard>get("mr:///sometest/{var:xx1,date,yyyyMMdd}").path();
    path = wb.<MRTableShard>get("mr:///sometest/{var:xx1,date,yyyyMMdd}_test").path();
    Assert.assertEquals("sometest/20140801_test",path);
    wb.set("var:xx2", "sometest/{var:xx1,date,yyyyMMdd}");
    path = wb.<MRTableShard>get("mr:///{var:xx2}_test").path();
    Assert.assertEquals("sometest/20140801_test",path);
  }

  @Test
  public void testResolve2() {
    final MREnv env = LocalMREnv.createTemp();
    final MRWhiteboard wb = new MRWhiteboardImpl(env, "proc", "none");

    wb.set("var:xx1", new Date(2014-1900,7,1));
    wb.set("var:xx2", "sometest/{var:xx1,date,yyyyMMdd}");
    String path = wb.<MRTableShard>get("mr:///{var:xx2}_test").path();
    Assert.assertEquals("sometest/20140801_test",path);
  }


  @SuppressWarnings("UnusedDeclaration")
  @MRProcessClass(goal = {"temp:mr:///split1_tmp", "temp:mr:///split2_tmp", "temp:mr:///split3_tmp"})
  public static final class SampleSplitter {
    private static final Random rnd = new Random(0xdeadbeef);

    public SampleSplitter(MRState state){}

    @SuppressWarnings("UnusedDeclaration")
    @MRMapMethod(
            input = "mr:///mobilesearchtest/20141017_655_11",
            output = {
                    "temp:mr:///split1_tmp",
                    "temp:mr:///split2_tmp",
                    "temp:mr:///split3_tmp"
            })
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      int v = rnd.nextInt();
      final int i = Math.abs(v % 3);
      switch (i) {
        case 0:
          output.add(0, "" + v, "#", "" + v);
          break;
        case 1:
          output.add(1, "" + v, "#", "" + v);
          break;
        case 2:
          output.add(2, "" + v, "#", "" + v);
          break;
      }
    }
  }

  @Test
  public void testSplit() {
    final ProcessRunner runner = new SSHProcessRunner("batista", "/Berkanavt/mapreduce/bin/mapreduce-dev");
    final MREnv env = new YaMREnv(runner, "mobilesearch", "cedar:8013");
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(SampleSplitter.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
  }

  /////////////////////
  //
  private static int LIMIT = 3;
  private static final String GOALS = "mr:///mobilesearchtest/{var:user}/split{var:array1_10}_tmp";
  @SuppressWarnings("UnusedDeclaration")
  @MRProcessClass(goal = {GOALS})
  public static final class SampleSplitter1 {
    private static final Random rnd = new Random(0xdeadbeef);

    public SampleSplitter1(MRState state){}

    @SuppressWarnings("UnusedDeclaration")
    @MRMapMethod(
        input = "mr:///mobilesearchtest/20141017_655_11",
        output = {
            GOALS
        })
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      int v = rnd.nextInt();
      int i = Math.abs(v % LIMIT);
      output.add(i, "" + i, "" + v, "" + v);
    }
  }

  @Test
  public void testArrays1() {
    final ProcessRunner runner = new SSHProcessRunner("batista", "/Berkanavt/mapreduce/bin/mapreduce-dev");
    final MREnv env = new YaMREnv(runner, "mobilesearch", "cedar:8013");
    final Properties vars = new Properties();
    vars.put("var:user", System.getProperty("user.name"));

    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < LIMIT; ++i) {
      sb.append(i);
      if (i < LIMIT - 1) sb.append(',');
    }

    vars.put("var:array1_10", (Object)sb.toString().split(","));

    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(SampleSplitter1.class, env, vars);
    mrProcess.wb().wipe();
    mrProcess.execute();
  }

  //////////////////////
  ///
  ///
  @SuppressWarnings("UnusedDeclaration")
  @MRProcessClass(goal = {
      "mr:///mobilesearchtest/{var:user}/split{var:array1_10}_tmp"
      /*"mr:///mobilesearchtest/@(1..10)",
      "mr:///mobilesearchtest/{var:user}/split@(1..10)_tmp"/*/})
  public static final class ArraysTest0 {
    public ArraysTest0(MRState state){}
  }

  @Test
  public void testArrays0() {
    //final ProcessRunner runner = new SSHProcessRunner("batista", "/Berkanavt/mapreduce/bin/mapreduce-dev");
    final MREnv env = new LocalMREnv(System.getProperty("user.home"));
    final Properties vars = new Properties();
    vars.put("var:user", System.getProperty("user.name"));

    final StringBuilder sb = new StringBuilder();
    int limit = 3;
    for (int i = 0; i < limit; ++i) {
      sb.append(i);
      if (i < limit - 1) sb.append(',');
    }

    vars.put("var:array1_10", (Object)sb.toString().split(","));

    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(ArraysTest0.class, env, vars);
    mrProcess.wb().wipe();
    //mrProcess.execute();
  }
}
