package runner;

import org.junit.Test;
import com.expleague.yasm4u.domains.mr.env.LocalProcessRunner;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

import static junit.framework.TestCase.assertEquals;

/**
 * Created by minamoto on 04/03/15.
 */
public class LocalRunnerTest {
  @Test
  public void shouldNotFail() throws IOException, InterruptedException {
    LocalProcessRunner runner = new LocalProcessRunner("echo");
    Process proc = runner.start("ok");
    LineNumberReader reader = new LineNumberReader(new InputStreamReader(proc.getInputStream()));
    assertEquals("ok", reader.readLine());
    int rc = proc.waitFor();
    assertEquals(rc, 0);
  }

  @Test
  public void shouldNotFailManyParams() throws IOException, InterruptedException {
    LocalProcessRunner runner = new LocalProcessRunner("echo");
    Process proc = runner.start("1", "2", "3", "4");
    LineNumberReader reader = new LineNumberReader(new InputStreamReader(proc.getInputStream()));
    assertEquals("1 2 3 4", reader.readLine());
    int rc = proc.waitFor();
    assertEquals(rc, 0);
  }
}
