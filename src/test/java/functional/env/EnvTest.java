package functional.env;

import functional.BaseMRTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.MRTableState;
import solar.mr.proc.Whiteboard;
import solar.mr.proc.impl.MRPath;
import solar.mr.proc.impl.WhiteboardImpl;

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
    MRTableState notExists = null;
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
    wb.<Long>set("var:long", 1l);
    assertEquals(1l, wb.snapshot().get("var:long"));

    wb.<Long>set("var:long",6 * 1l);
    assertEquals(6l, wb.snapshot().get("var:long"));
  }
}
