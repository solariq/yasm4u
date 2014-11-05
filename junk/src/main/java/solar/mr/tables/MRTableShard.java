package solar.mr.tables;

import java.util.Date;


import solar.mr.MREnv;
import solar.mr.MRTable;

/**
* User: solar
* Date: 15.10.14
* Time: 10:42
*/
public interface MRTableShard {
  String path();
  MREnv container();
  MRTable owner();

  long metaTS();
  boolean isAvailable();
  String crc();
}
