package solar.mr.env.sample;

import com.spbsu.commons.random.FastRandom;
import solar.mr.MROutput;
import solar.mr.proc.State;
import solar.mr.proc.impl.MRPath;
import solar.mr.routines.MRMap;

/**
 * User: solar
 * Date: 17.11.14
 * Time: 10:30
 */
public class KeysSampleMap extends MRMap {
  private final FastRandom rng = new FastRandom();
  private final double probability;
  @SuppressWarnings("ConstantConditions")
  public KeysSampleMap(MRPath[] input, MROutput out, State state) {
    super(input, out, state);
    probability = state.<Double>get("var:probability");
  }

  @Override
  public void map(MRPath table, String sub, CharSequence value, String key) {
    rng.setSeed(key.hashCode());
    if (rng.nextDouble() < probability)
      output.add(key, sub.length() == 0 ? "#" : sub, value);
  }
}