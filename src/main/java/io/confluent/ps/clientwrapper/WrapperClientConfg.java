package io.confluent.ps.clientwrapper;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;

@Data
public class WrapperClientConfg {

  String appId;
  Map configs = new HashMap();

}
