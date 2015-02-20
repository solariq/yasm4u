package solar.mr.env;

import com.spbsu.commons.seq.CharSeqTools;
import solar.mr.MRErrorsHandler;
import solar.mr.MRRoutine;
import solar.mr.proc.impl.MRPath;
import solar.mr.routines.MRRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
* User: solar
* Date: 30.01.15
* Time: 13:36
*/
public class ErrorsTableHandler extends MRRoutine {
  private final MRErrorsHandler errorsHandler;

  public ErrorsTableHandler(MRPath errorsShardName, MRErrorsHandler errorsHandler) {
    super(new MRPath[]{errorsShardName}, null, null);
    this.errorsHandler = errorsHandler;
  }

  @Override
  public void process(final MRRecord record) {
    CharSequence[] parts = CharSeqTools.split(record.value, '\t', new CharSequence[4]);
    errorsHandler.error(record.key, record.sub, new MRRecord(MRPath.create(parts[0].toString()), parts[1].toString(), parts[2].toString(), parts[3]));
    try {
      final Class clazz = Class.forName(record.key);
      if (clazz.isAssignableFrom(Throwable.class)){
        Throwable th = (Throwable)new ObjectInputStream(new ByteArrayInputStream(CharSeqTools.parseBase64(record.value))).readObject();
        th.printStackTrace(System.err);
      }
    } catch (ClassNotFoundException e) {
      System.err.println(record.value);
      System.err.println(record.key + "\t" + record.sub.replace("\\n", "\n").replace("\\t", "\t"));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
