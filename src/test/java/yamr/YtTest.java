package yamr;

import org.junit.Test;
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.mr.env.ProcessRunner;
import ru.yandex.se.yasm4u.domains.mr.env.SSHProcessRunner;
import ru.yandex.se.yasm4u.domains.mr.env.YtMREnv;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.AnnotatedMRProcess;
import ru.yandex.se.yasm4u.domains.wb.Whiteboard;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.wb.impl.WhiteboardImpl;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRMapMethod;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRProcessClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by minamoto on 17/11/14.
 */
public class YtTest {

  private final static String GOALS = "mr:////tmp/minamoto/split{var:array}_tmp";
  private static int LIMIT = 3;
  @MRProcessClass(goal = {GOALS})
  public static final class SampleSplitter {

    private final State state;
    private static final Random rnd = new Random(0xdeadbeef);

    public SampleSplitter(State state) {
      this.state = state;
    }

    @MRMapMethod(
        input = "mr://{var:tmp}/minamoto/test2",
        output = {GOALS})
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
      //throw new RuntimeException("fun!");
    }
  }

  @Test
  public void testSplit() {
    final ProcessRunner runner = new SSHProcessRunner("testing.mobsearch.serp.yandex.ru", "/usr/bin/yt");
    final MREnv env = new YtMREnv(runner, "minamoto", "plato.yt.yandex.net");

    Whiteboard wb = new WhiteboardImpl(env, SampleSplitter.class.getName());
    List<String> array = new ArrayList<>();
    for (int i = 0; i < LIMIT; ++i) {
      array.add(Integer.toString(i));
    }
    wb.set("var:array", array.toArray(new String[array.size()]));
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(SampleSplitter.class, wb, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
  }

  @Test
  public void testList() {
    final ProcessRunner runner = new SSHProcessRunner("testing.mobsearch.serp.yandex.ru", "/usr/bin/yt");
    final MREnv env = new YtMREnv(runner, "minamoto", "plato.yt.yandex.net");
    env.list(MRPath.create("/tmp/minamoto"));
    env.list(MRPath.create("/tmp/minamoto/test1"));
    env.list(MRPath.create("/"));
  }

}
