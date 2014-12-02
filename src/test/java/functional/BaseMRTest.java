package functional;

import org.junit.runners.Parameterized;
import solar.mr.MREnv;
import solar.mr.env.LocalMREnv;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created by inikifor on 02.12.14.
 */
public class BaseMRTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {new LocalMREnv()}
    });
  }

  @Parameterized.Parameter
  public MREnv env; // should be public

  protected String tableName() {
    return "temp/yasm4u-tests/" + this.getClass().getName();
  }

}
