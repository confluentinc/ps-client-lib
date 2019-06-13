package io.confluent.ps.clientwrapper;

import com.fasterxml.jackson.annotation.JsonMerge;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;

@Data
public class WrapperClientConfg {

  String appId;
  @JsonMerge
  Map configs = new HashMap();

}
