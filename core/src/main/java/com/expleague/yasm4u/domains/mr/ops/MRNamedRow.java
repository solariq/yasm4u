package com.expleague.yasm4u.domains.mr.ops;

import com.expleague.commons.seq.CharSeq;
import com.expleague.commons.seq.CharSeqBuilder;
import com.expleague.commons.seq.CharSeqTools;
import com.expleague.ml.data.tools.CsvRow;
import com.expleague.ml.data.tools.WritableCsvRow;
import com.expleague.yasm4u.domains.mr.MRPath;

import java.util.Optional;

/**
* User: solar
* Date: 05.11.14
* Time: 15:58
*/
public class MRNamedRow extends MRRecord implements CsvRow {
  private final CsvRow delegate;

  public MRNamedRow(final MRPath source, String key, String sub, CsvRow delegate) {
    super(source, key, sub, delegate.toString());
    this.delegate = delegate;
  }

  @Override
  public CsvRow names() {
    return delegate.names();
  }

  @Override
  public MRNamedRow clone() {
    return new MRNamedRow(source, key, sub, delegate.clone());
  }

  @Override
  public CharSeq at(int i) {
    return delegate.at(i);
  }

  @Override
  public Optional<CharSeq> apply(String s) {
    return delegate.apply(s);
  }
}
