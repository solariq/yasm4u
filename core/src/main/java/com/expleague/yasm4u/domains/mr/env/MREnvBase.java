package com.expleague.yasm4u.domains.mr.env;

import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.Routine;
import com.expleague.yasm4u.domains.mr.MREnv;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.routines.MergeRoutine;
import com.expleague.yasm4u.domains.mr.routines.SortRoutine;

import java.util.List;

/**
 * User: solar
 * Date: 27.03.15
 * Time: 16:51
 */
public abstract class MREnvBase implements MREnv {
  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    routines.add(new MergeRoutine());
    routines.add(new SortRoutine());
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
    parser.registerProtocol("mr", from -> MRPath.createFromURI("mr:" + from));
  }
}
