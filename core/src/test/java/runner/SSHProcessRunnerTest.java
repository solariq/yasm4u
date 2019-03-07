package runner;

import com.expleague.commons.io.StreamTools;
import com.expleague.commons.util.logging.Interval;
import org.junit.Assert;
import org.junit.Test;
import com.expleague.yasm4u.domains.mr.env.ProcessRunner;
import com.expleague.yasm4u.domains.mr.env.SSHProcessRunner;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * This test is for manual handling only, don't try it using automatically
 * User: solar
 * Date: 28.02.15
 * Time: 7:09
 */
@SuppressWarnings("unused")
public abstract class SSHProcessRunnerTest {
  public static final String PROXY_HOST = "expleague.com";

  /**
   * this test must hang out with
   * [ssh: Could not resolve hostname fakehost.local: nodename nor servname provided, or not known]
   * message popping out
   */
  @Test
  public void testNoRootToHost() {
    final ProcessRunner runner = new SSHProcessRunner("fakehost.local", "/dev/null");
    runner.start("echo", "Ok");
  }

  @Test
  public void testShortConnection() {
    final ProcessRunner runner = new SSHProcessRunner(PROXY_HOST, "bash");
    runner.start("-c", "echo Ok");
  }

  /**
   * Must be less then 10 seconds on stable connection
   */
  @Test
  public void testRTTime() {
    final ProcessRunner runner = new SSHProcessRunner(PROXY_HOST, "bash");
    Interval.start();
    for(int i = 0; i < 100; i++) {
      runner.start("-c", "echo Ok");
    }
    Interval.stopAndPrint("100 connections to " + PROXY_HOST + " time");
  }

  /**
   * Run test, switch off network, wait 60 seconds, switch the network back. Everything must be fine.
   * To test the clean up, run the test, check the remote process, stop the test, check, that remote process is dead
   */
  @Test
  public void testConnectionBlink() throws IOException, InterruptedException {
    final ProcessRunner runner = new SSHProcessRunner(PROXY_HOST, "bash");
    final Process start = runner.start("-c", "sleep 60; echo Ok");
    //noinspection unchecked
    new FutureTask(() -> {
      StreamTools.transferData(start.getErrorStream(), System.err);
      return null;
    });

    final CharSequence result = StreamTools.readStream(start.getInputStream());
    Assert.assertEquals("Ok\n", result.toString());
    start.waitFor();
  }
}
