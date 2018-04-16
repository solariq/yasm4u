package functional.env;

import functional.BaseMRTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import com.expleague.yasm4u.domains.mr.ops.impl.MRTableState;
import com.expleague.yasm4u.domains.wb.Whiteboard;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.wb.impl.WhiteboardImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Created by minamoto on 18/02/15.
 */
@RunWith(Parameterized.class)
public class EnvTest extends BaseMRTest {
  @Test
  public void resolveNotExistantShouldntBeNull() {
    MRTableState notExists;
    assertNotNull(notExists = env.resolve(MRPath.create("/tmp/__not_exists")));
    assertFalse(notExists.isAvailable());
  }
   
  @Test(expected = IllegalArgumentException.class)
  public void resolveNotExistantDirectory() {
    env.resolve(MRPath.create("/tmp/__not_exists/"));
  }

  @Test
  public void conversions() {
    Whiteboard wb = new WhiteboardImpl(env, "conversions");
    wb.set("var:long", 1L);
    assertEquals(1L, (long)wb.snapshot().get("var:long"));

    wb.set("var:long", 6 * 1L);
    assertEquals(6L, (long)wb.snapshot().get("var:long"));
  }
}
