package yamr;

import org.junit.Test;
import solar.mr.MREnv;
import solar.mr.MROutput;
import solar.mr.env.ProcessRunner;
import solar.mr.env.SSHProcessRunner;
import solar.mr.env.YaMREnv;
import solar.mr.env.YtMREnv;
import solar.mr.proc.MRState;
import solar.mr.proc.impl.AnnotatedMRProcess;
import solar.mr.proc.tags.MRMapMethod;
import solar.mr.proc.tags.MRProcessClass;

import java.util.Random;

/**
 * Created by minamoto on 17/11/14.
 */
public class YtTest {
  @MRProcessClass(goal = {"temp:mr:////tmp/minamoto/split1_tmp", "temp:mr:////tmp/minamoto/split2_tmp", "temp:mr:////tmp/minamoto/split3_tmp"})
  public static final class SampleSplitter {

    private final MRState state;
    private static final Random rnd = new Random(0xdeadbeef);

    public SampleSplitter(MRState state) {
      this.state = state;
    }

    @MRMapMethod(
        input = "mr:////tmp/minamoto/test2",
        output = {
            "temp:mr:////tmp/minamoto/split1_tmp",
            "temp:mr:////tmp/minamoto/split2_tmp",
            "temp:mr:////tmp/minamoto/split3_tmp"
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
    final ProcessRunner runner = new SSHProcessRunner("testing.mobsearch.serp.yandex.ru", "/usr/bin/yt");
    final MREnv env = new YtMREnv(runner, "minamoto", "plato.yt.yandex.net");
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(SampleSplitter.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
  }

  @Test
  public void testList() {
    final ProcessRunner runner = new SSHProcessRunner("testing.mobsearch.serp.yandex.ru", "/usr/bin/yt");
    final MREnv env = new YtMREnv(runner, "minamoto", "plato.yt.yandex.net");
    env.list("//tmp/minamoto");
    env.list("//tmp/minamoto/test1");
  }

}
