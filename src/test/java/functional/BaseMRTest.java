package functional;

import org.junit.runners.Parameterized;
import solar.mr.MREnv;
import solar.mr.env.*;
import solar.mr.proc.impl.MRPath;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created by inikifor on 02.12.14.
 */
public class BaseMRTest {

  /** 
   * please, dont change TABLE_NAME_PREFIX, Yt has several places where mobilesearch user  has rw access 
   * that not a problem in YaMR. 
   */
  public static final String TABLE_NAME_PREFIX = "mr:///home/yasm4u-tests/";
  public static final String SALT = "_test1"; // Don't know how to implement this better yet

  public static final ProcessRunner YAMR_RUNNER = new SSHProcessRunner("prod1-test-mob.serp.yandex.ru", "/Berkanavt/mapreduce/bin/mapreduce-dev");
  public static final String YAMR_USER = "mobilesearch";
  public static final String YAMR_CLUSTER = "cedar:8013";
  public static final ProcessRunner YTMR_RUNNER =  new SSHProcessRunner("prod1-test-mob.serp.yandex.ru", "/usr/bin/yt");
  public static final String YTMR_USER = "minamoto";
  public static final String YTMR_CLUSTER = "aristotle.yt.yandex.net";

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {LocalMREnv.createTemp()},
        {new CompositeMREnv(new YaMREnv(YAMR_RUNNER, YAMR_USER, YAMR_CLUSTER), LocalMREnv.createTemp())},
        //{new ProfilerMREnv(new YaMREnv(YAMR_RUNNER, YAMR_USER, YAMR_CLUSTER))},
        //{new CompositeMREnv(new YtMREnv(YTMR_RUNNER, YTMR_USER, YTMR_CLUSTER), LocalMREnv.createTemp())}
    });
  }

  @Parameterized.Parameter
  public MREnv env; // should be public

}
