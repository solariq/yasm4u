package solar.mr;

import solar.mr.proc.State;

import java.util.Map;

/**
 * Created by inikifor on 30.11.14.
 */
public interface ProfilableMREnv extends MREnv {

  ProfilableMREnv.Profiler EMPTY_PROFILER = new Profiler() {

    @Override
    public boolean isEnabled() {
      return false;
    }

    @Override
    public String getTableName() {
      return "";
    }

    @Override
    public void addExecutionStatistics(Map<String, Integer> timePerHosts) {
      // empty
    }

    @Override
    public MREnv getPofilableEnv() {
      return null;
    }


  };

  boolean execute(Class<? extends MRRoutine> exec, State state, MRTableShard[] in, MRTableShard[] out, final MRErrorsHandler errorsHandler, final Profiler profiler);

  MRTableShard[] resolveAll(String[] strings, final Profiler profiler);

  MRTableShard resolve(String path, final Profiler profiler);

  interface Profiler {

    boolean isEnabled();

    String getTableName();

    public void addExecutionStatistics(Map<String, Integer> timePerHosts);

    MREnv getPofilableEnv();

  }

}
