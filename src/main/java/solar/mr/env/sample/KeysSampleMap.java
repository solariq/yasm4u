package solar.mr.env.sample;

import com.spbsu.commons.random.FastRandom;
import solar.mr.MROutput;
import solar.mr.proc.MRState;
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
  public KeysSampleMap(String[] input, MROutput out, MRState state) {
    super(input, out, state);
    probability = state.<Double>get("var:probability");
  }

  @Override
  public void map(String key, String sub, CharSequence value) {
    rng.setSeed(key.hashCode());
    if (rng.nextDouble() < probability)
      output.add(key, sub.length() == 0 ? "#" : sub, value);
  }
}
