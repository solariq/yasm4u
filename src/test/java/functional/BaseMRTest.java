package functional;

import org.junit.runners.Parameterized;
import solar.mr.MREnv;
import solar.mr.env.*;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created by inikifor on 02.12.14.
 */
public class BaseMRTest {

  public static final String TABLE_NAME_PREFIX = "temp/yasm4u-tests/";
  public static final String SALT = "_inikifor"; // Don't know how to implement this better yet
  public static final String SCHEMA = "mr:///";

  public static final ProcessRunner YAMR_RUNNER = new SSHProcessRunner("batista", "/Berkanavt/mapreduce/bin/mapreduce-dev");
  public static final String YAMR_USER = "mobilesearch";
  public static final String YAMR_CLUSTER = "cedar:8013";

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {LocalMREnv.createTemp()},
        {new YaMREnv(YAMR_RUNNER, YAMR_USER, YAMR_CLUSTER)}
    });
  }

  @Parameterized.Parameter
  public MREnv env; // should be public

}