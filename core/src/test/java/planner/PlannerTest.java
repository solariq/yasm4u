package planner;

import com.expleague.commons.util.ArrayTools;
import com.expleague.yasm4u.*;
import org.junit.Assert;
import org.junit.Test;
import com.expleague.yasm4u.impl.MainThreadJES;
import com.expleague.yasm4u.impl.Planner;

import java.net.URI;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.Function;

/**
 * User: solar
 * Date: 14.04.15
 * Time: 21:27
 */
public class PlannerTest {
  private static class EmptyJoba extends Joba.Stub {

    protected EmptyJoba(Ref<?, ?>[] consumes, Ref<?, ?>[] produces) {
      super(consumes, produces);
    }

    @Override
    public void run() {
    }
  }

  @Test
  public void testLinearJobas() {
    final FakeRef a = new FakeRef(1);
    final FakeRef b = new FakeRef(2);
    final FakeRef c = new FakeRef(3);
    final Planner p = new Planner(new Ref[0], new Routine[0], new Joba[]{
            new EmptyJoba(new Ref[]{}, new Ref[]{a}),
            new EmptyJoba(new Ref[]{a}, new Ref[]{b}),
            new EmptyJoba(new Ref[]{b}, new Ref[]{c})
    });

    final Joba[] plan = p.build(new MainThreadJES(), c);
    Assert.assertEquals(3, plan.length);
  }

  @Test
  public void testLinearRoutines() {
    final FakeRef a = new FakeRef(1);
    final FakeRef b = new FakeRef(2);
    final FakeRef c = new FakeRef(3);
    final Planner p = new Planner(new Ref[0], new Routine[]{
            new Routine() {
              @Override
              public Joba[] buildVariants(Ref[] state, JobExecutorService executor) {
                final List<Joba> result = new ArrayList<>();
                if (ArrayTools.indexOf(a, state) >= 0)
                  result.add(new EmptyJoba(new Ref[]{a}, new Ref[]{b}));
                if (ArrayTools.indexOf(b, state) >= 0)
                  result.add(new EmptyJoba(new Ref[]{b}, new Ref[]{c}));
                return result.toArray(new Joba[result.size()]);
              }
            }
    }, new Joba[]{
            new EmptyJoba(new Ref[]{}, new Ref[]{a}),
    });

    final Joba[] plan = p.build(new MainThreadJES(), c);
    Assert.assertEquals(3, plan.length);
  }

  @Test
  public void testBetterWay() {
    final FakeRef a = new FakeRef(1);
    final FakeRef b = new FakeRef(2);
    final FakeRef c = new FakeRef(3);
    final Planner p = new Planner(new Ref[0], new Routine[0], new Joba[]{
            new EmptyJoba(new Ref[]{}, new Ref[]{a}),
            new EmptyJoba(new Ref[]{a}, new Ref[]{b}),
            new EmptyJoba(new Ref[]{b}, new Ref[]{c}),
            new EmptyJoba(new Ref[]{}, new Ref[]{c})
    });

    final Joba[] plan = p.build(new MainThreadJES(), c);
    Assert.assertEquals(1, plan.length);
  }

  @Test
  public void testSort100Tables() {
    final Planner p = new Planner(new Ref[]{
            new FakeRef(0)
    }, new Routine[]{
            new SequentialRoutine(0, 100),
            new Routine() {
              @Override
              public Joba[] buildVariants(Ref[] state, JobExecutorService executor) {
                return ArrayTools.map(state, Joba.class, argument -> {
                  final int index = ((FakeRef) argument).index;
                  if (index > 0)
                    return new EmptyJoba(new Ref[]{argument}, new Ref[]{
                            new FakeRef(-index)});
                  return new EmptyJoba(new Ref[]{argument}, new Ref[]{});
                });
              }
            }
    }, new Joba[]{});

    for(int i = 0; i < 100; i++) {
      final Joba[] plan = p.build(new MainThreadJES(), new FakeRef(100));
      Assert.assertEquals(100, plan.length);
    }
  }
  @Test
  public void testRandomOrderMerge() {
    final Planner p = new Planner(new Ref[0], new Routine[]{
            new CreateFromAirRoutine(100),
            new MergeRangeRoutine(100)
    }, new Joba[]{});

    for(int i = 0; i < 100; i++) {
      final Joba[] plan = p.build(new MainThreadJES(), new FakeRef(100));
      Assert.assertEquals(101, plan.length);
    }
  }

  @Test
  public void testDoubleRandomOrderMerge() {
    final Planner p = new Planner(new Ref[0], new Routine[]{
            new CreateFromAirRoutine(100),
            new MergeRangeRoutine(100),
            new CreateFromAirRoutine(101, 200, new FakeRef(100)),
            new MergeRangeRoutine(101, 200),
    }, new Joba[]{});

    for(int i = 0; i < 100; i++) {
      final Joba[] plan = p.build(new MainThreadJES(), new FakeRef(200));
      Assert.assertEquals(201, plan.length);
    }
  }

  /**
   * Two parallel tasks one consumes results of another
   */
  @Test
  public void testParallelConsuming() {
    final Planner p = new Planner(new Ref[0], new Routine[]{
            new MergeRangeRoutine(3),
            new CreateFromAirRoutine(1),
            new SequentialRoutine(0, 2),
            new SequentialRoutine(3, 4),
    }, new Joba[]{});
    final Joba[] plan = p.build(new MainThreadJES(), new FakeRef(4));
  }

  private static class MergeRangeRoutine implements Routine {
    private final int from;
    private final int to;

    private MergeRangeRoutine(int n) {
      this(0, n);
    }
    
    private MergeRangeRoutine(int from, int to) {
      this.from = from;
      this.to = to;
    }

    @Override
    public Joba[] buildVariants(Ref[] state, JobExecutorService executor) {
      final BitSet bitSet = new BitSet(to-from);
      bitSet.set(0, to-from);
      List<Ref> deps = new ArrayList<>();
      for (Ref ref : state) {
        final int index = ((FakeRef) ref).index;
        if (index >= from && index < to) {
          bitSet.clear(index - from);
          deps.add(ref);
        }
      }
      if (bitSet.isEmpty())
        return new Joba[]{new EmptyJoba(deps.toArray(new Ref[deps.size()]), new Ref[]{new FakeRef(to)})};
      return new Joba[0];
    }
  }

  private static class CreateFromAirRoutine implements Routine {
    private final Ref[] consumes;
    private final int from;
    private final int to;

    public CreateFromAirRoutine(int n) {
      this(0, n);
    }
      
    public CreateFromAirRoutine(int from, int to, Ref... consumes) {
      this.consumes = consumes;
      this.from = from;
      this.to = to;
    }

    @Override
    public Joba[] buildVariants(Ref[] state, JobExecutorService executor) {
      final List<Joba> result = new ArrayList<>();
      for (int i = from; i < to; i++) {
        result.add(new EmptyJoba(consumes, new Ref[]{new FakeRef(i)}));
      }
      return result.toArray(new Joba[result.size()]);
    }
  }

  private static class SequentialRoutine implements Routine {
    private final int to;
    private final int from;

    public SequentialRoutine(int from, int to) {
      this.to = to;
      this.from = from;
    }

    @Override
    public Joba[] buildVariants(Ref[] state, JobExecutorService executor) {
      final List<Joba> result = new ArrayList<>();
      for (int i = 0; i < state.length; i++) {
        final Ref ref = state[i];
        final int index = ((FakeRef) ref).index;
        if (index >= 0 && index < to && index >= from)
          result.add(new EmptyJoba(new Ref[]{ref}, new Ref[]{new FakeRef(index + 1)}));
      }
      return result.toArray(new Joba[result.size()]);
    }
  }

  private static class FakeRef implements Ref {

    private final int index;

    private FakeRef(int index) {
      this.index = index;
    }

    @Override
    public URI toURI() {
      return URI.create("fake:" + index);
    }

    @Override
    public Class type() {
      return Void.class;
    }

    @Override
    public Class domainType() {
      return Domain.class;
    }

    @Override
    public Object resolve(Domain controller) {
      return null;
    }

    @Override
    public boolean available(Domain controller) {
      return false;
    }

    @Override
    public String toString() {
      return "" + index;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof FakeRef)) return false;

      FakeRef fakeRef = (FakeRef) o;

      if (index != fakeRef.index) return false;

      return true;
    }
    @Override
    public int hashCode() {
      return index;
    }

  }
}