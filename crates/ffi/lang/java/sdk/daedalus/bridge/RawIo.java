package daedalus.bridge;

import java.util.List;

public interface RawIo {
  void push(String port, Object value);

  void pushMany(String port, List<?> values);
}

