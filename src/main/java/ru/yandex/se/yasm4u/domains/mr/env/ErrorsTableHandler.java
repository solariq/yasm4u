package ru.yandex.se.yasm4u.domains.mr.env;

import com.spbsu.commons.seq.CharSeqTools;
import ru.yandex.se.yasm4u.domains.mr.MRErrorsHandler;
import ru.yandex.se.yasm4u.domains.mr.ops.impl.MROperation;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
* User: solar
* Date: 30.01.15
* Time: 13:36
*/
public class ErrorsTableHandler extends MROperation {
  private final MRErrorsHandler errorsHandler;
  private int count;

  public ErrorsTableHandler(MRPath errorsShardName, MRErrorsHandler errorsHandler) {
    super(new MRPath[]{errorsShardName}, null, null);
    this.errorsHandler = errorsHandler;
  }

  @Override
  public void process(final MRRecord record) {
    count++;
    CharSequence[] parts = CharSeqTools.split(record.value, '\t', new CharSequence[4]);
    final MRRecord realRecord = new MRRecord(MRPath.create(parts[0].toString()), parts[1].toString(), parts[2].toString(), parts[3]);
    try {
      final Class clazz = Class.forName(record.key);
      if (Throwable.class.isAssignableFrom(clazz)){
        final Throwable th = (Throwable)new ObjectInputStream(new ByteArrayInputStream(CharSeqTools.parseBase64(record.sub))).readObject();
        errorsHandler.error(th, realRecord);
        return;
      }
    } catch (ClassNotFoundException e) {
      // skip
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    errorsHandler.error(record.key, record.sub, realRecord);
  }

  public int errorsCount() {
    return count;
  }
}
