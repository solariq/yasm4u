package yamr;

import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.mr.env.*;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MRTableState;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.AnnotatedMRProcess;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;
import ru.yandex.se.yasm4u.domains.wb.impl.WhiteboardImpl;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRMapMethod;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRProcessClass;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRRead;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRReduceMethod;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

import java.util.Date;
import java.util.Iterator;
import java.util.Random;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:35
 */
public class MRProcTest {

  private final static String TEST_SERVER_PROXY = "dodola";
  private final static String TEST_MR_USER = "mobilesearch";

  @SuppressWarnings("UnusedDeclaration")
  @MRProcessClass(goal = "var:result")
  public static class SAPPCounter {
    public SAPPCounter(State state) {}

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
    public int count(Iterator<MRRecord> records) {
      int result = 0;
      while (records.hasNext()) {
        final MRRecord next = records.next();
        if ("SAPP".equals(next.key))
          result = CharSeqTools.parseInt(next.value);
      }
      return result;
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  @MRProcessClass(goal = "var:result")
  public static class FailAtRandomMap {
    private final State state;
    private final Random rng = new FastRandom();
    int index = 0;

    public FailAtRandomMap(State state) {
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
    public int poh(Iterator<MRRecord> line) {
      return 0;
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  @MRProcessClass(goal = "var:result")
  public static class FailAtRandomReduce {
    private final State state;
    private final Random rng = new FastRandom();
    int index = 0;

    public FailAtRandomReduce(State state) {
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
    public int poh(Iterator<MRRecord> line) {
      return 0;
    }
  }


  @Test
  public void testProcCreate() {
    final ProcessRunner runner = new SSHProcessRunner(TEST_SERVER_PROXY, "/Berkanavt/mapreduce/bin/mapreduce-dev");
    final MREnv env = new CompositeMREnv(new YaMREnv(runner, TEST_MR_USER, "cedar:8013"));
    final Whiteboard wb = new WhiteboardImpl(env, "SAPPCounter");
//    wb.wipe();
    wb.set("var:date", new Date(2014-1900, 8, 1));
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(SAPPCounter.class, wb, env);
    int count = mrProcess.<Integer>result();
    Assert.assertEquals(2611709, count);
    mrProcess.wb().wipe();
  }

  @Test
  public void testExceptionMap() {
    final ProcessRunner runner = new SSHProcessRunner(TEST_SERVER_PROXY, "/Berkanavt/mapreduce/bin/mapreduce-dev");
    final MREnv env = new YaMREnv(runner, TEST_MR_USER, "cedar:8013");
    final Whiteboard initial = new WhiteboardImpl(env, FailAtRandomReduce.class.getName());
    initial.set("var:date", new Date(2014 - 1900, 8, 1));
    initial.set("var:delay", 10000);
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(FailAtRandomReduce.class, env);
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
    final Whiteboard wb = new WhiteboardImpl(env, "proc");
    wb.set("var:xxx", "yyy");
    final String resolveString = wb.get("{var:xxx}");
    @SuppressWarnings("UnusedDeclaration")
    final MRTableState resolveTable = wb.get("mr://xxx");

    Assert.assertEquals("yyy", resolveString);

    wb.set("var:xx1", new Date(2014-1900,7,1));
    Assert.assertEquals("20140801", wb.get("{var:xx1,date,yyyyMMdd}"));
    @SuppressWarnings("UnusedAssignment")
    String path = wb.<MRTableState>get("mr:///sometest/{var:xx1,date,yyyyMMdd}").path();
    path = wb.<MRTableState>get("mr:///sometest/{var:xx1,date,yyyyMMdd}_test").path();
    Assert.assertEquals("sometest/20140801_test",path);
    wb.set("var:xx2", "sometest/{var:xx1,date,yyyyMMdd}");
    path = wb.<MRTableState>get("mr:///{var:xx2}_test").path();
    Assert.assertEquals("sometest/20140801_test",path);
  }

  @Test
  public void testResolve2() {
    final MREnv env = LocalMREnv.createTemp();
    final Whiteboard wb = new WhiteboardImpl(env, "proc");

    wb.set("var:xx1", new Date(2014-1900,7,1));
    wb.set("var:xx2", "sometest/{var:xx1,date,yyyyMMdd}");
    String path = wb.<MRTableState>get("mr:///{var:xx2}_test").path();
    Assert.assertEquals("sometest/20140801_test",path);
  }


  @SuppressWarnings("UnusedDeclaration")
  @MRProcessClass(goal = {"temp:mr:///split1_tmp", "temp:mr:///split2_tmp", "temp:mr:///split3_tmp"})
  public static final class SampleSplitter {
    private static final Random rnd = new Random(0xdeadbeef);

    public SampleSplitter(State state){}

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
  private static final String GOALS = "mr:///mobilesearchtest/split{var:array1_10}_tmp";
  @SuppressWarnings("UnusedDeclaration")
  @MRProcessClass(goal = {GOALS})
  public static final class SampleSplitter1 {
    private static final Random rnd = new Random(0xdeadbeef);

    final State state;
    public SampleSplitter1(State state){
      this.state = state;
      state.get("var:user");
    }

    @SuppressWarnings("UnusedDeclaration")
    @MRMapMethod(
        input = "mr:///mobilesearchtest/20141017_655_11",
        output = {GOALS})
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      //int v = rnd.nextInt();
      //int r = v % LIMIT;
      //int i = r > 0? r : -r;
      output.add(0, key, "hello!", value);
    }
  }

  @Test
  public void testArrays1() {
    final ProcessRunner runner = new SSHProcessRunner("batista", "/Berkanavt/mapreduce/bin/mapreduce-dev");
    final MREnv env = new YaMREnv(runner, "mobilesearch", "cedar:8013");
    final Whiteboard vars = new WhiteboardImpl(env, SampleSplitter1.class.getName());
    vars.set("var:user", System.getProperty("user.name"));

    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < LIMIT; ++i) {
      sb.append(i);
      if (i < LIMIT - 1) sb.append(',');
    }

    vars.set("var:array1_10", (Object) sb.toString().split(","));

    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(SampleSplitter1.class, vars, env);
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
    public ArraysTest0(State state){}
  }

  @Test
  public void testArrays0() {
    //final ProcessRunner runner = new SSHProcessRunner("batista", "/Berkanavt/mapreduce/bin/mapreduce-dev");
    final MREnv env = new LocalMREnv(System.getProperty("user.home"));
    final Whiteboard vars = new WhiteboardImpl(env, ArraysTest0.class.getName());
    vars.set("var:user", System.getProperty("user.name"));

    final StringBuilder sb = new StringBuilder();
    int limit = 3;
    for (int i = 0; i < limit; ++i) {
      sb.append(i);
      if (i < limit - 1) sb.append(',');
    }

    vars.set("var:array1_10", (Object)sb.toString().split(","));

    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(ArraysTest0.class, vars, env);
    mrProcess.wb().wipe();
    //mrProcess.execute();
  }
}
