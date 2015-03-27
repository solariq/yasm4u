package ru.yandex.se.yasm4u.domains.mr.io;

import com.spbsu.commons.func.Converter;
import com.spbsu.commons.seq.CharSeqTools;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

/**
 * User: solar
 * Date: 06.02.15
 * Time: 11:49
 */
@SuppressWarnings("UnusedDeclaration")
public class MRRecordConverter implements Converter<MRRecord, CharSequence> {
  @Override
  public CharSequence convertTo(MRRecord object) {
    return object.source.toString() + "\t" + object.toString();
  }

  @Override
  public MRRecord convertFrom(CharSequence source) {
    final CharSequence[] parts = new CharSequence[4];
    CharSeqTools.split(source, '\t', parts);
    return new MRRecord(MRPath.create(parts[0].toString()), parts[1].toString(), parts[2].toString(), parts[3]);
  }
}
