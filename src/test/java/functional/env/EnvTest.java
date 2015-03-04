package functional.env;

import functional.BaseMRTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.MRTableState;
import solar.mr.proc.impl.MRPath;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Created by minamoto on 18/02/15.
 */
@RunWith(Parameterized.class)
public class EnvTest extends BaseMRTest {
  @Test
  public void resolveNotExistantShouldntBeNull() {
    MRTableState notExists = null;
    assertNotNull(notExists = env.resolve(MRPath.create("/tmp/__not_exists")));
    assertFalse(notExists.isAvailable());
  }
   
  @Test(expected = IllegalArgumentException.class)
  public void resolveNotExistantDirectory() {
    env.resolve(MRPath.create("/tmp/__not_exists/"));
  }
}
