package ru.yandex.se.yasm4u.domains.mr.env;

import com.spbsu.commons.func.types.TypeConverter;
import ru.yandex.se.yasm4u.*;
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.routines.MergeRoutine;
import ru.yandex.se.yasm4u.domains.mr.routines.SortRoutine;

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
    parser.registerProtocol("mr", new TypeConverter<String, MRPath>() {
      @Override
      public MRPath convert(String from) {
        return MRPath.createFromURI("mr:" + from);
      }
    });
  }
}
