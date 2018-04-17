package com.expleague.yasm4u.domains.mr.routines;

import com.expleague.yasm4u.JobExecutorService;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.Routine;
import com.expleague.yasm4u.domains.mr.MREnv;
import com.expleague.yasm4u.domains.mr.MRPath;

import java.util.ArrayList;
import java.util.List;

/**
 * User: solar
 * Date: 07.11.14
 * Time: 16:39
 */
public class SortRoutine implements Routine {
  @Override
  public Joba[] buildVariants(Ref[] state, final JobExecutorService executor) {
    final List<Joba> variants = new ArrayList<>();
    for(int i = 0; i < state.length; i++) {
      if (MRPath.class.isAssignableFrom(state[i].type())) {
        final MRPath ref = (MRPath)executor.resolve(state[i]);
        variants.add(new Joba.Stub(new Ref[]{ref.mkunsorted()}, new Ref[]{ref.mksorted()}) {
          @Override
          public void run() {
            final MREnv env = executor.domain(MREnv.class);
            //noinspection unchecked
            env.sort((MRPath) consumes()[0].resolve(env));
          }
        });
      }
    }
    return variants.toArray(new Joba[variants.size()]);
  }
}
