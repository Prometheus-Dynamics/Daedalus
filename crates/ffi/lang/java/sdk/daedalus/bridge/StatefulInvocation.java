package daedalus.bridge;

import java.util.List;

public final class StatefulInvocation {
  public final List<Object> args;
  public final Object state;
  public final Object stateSpec;
  public final Extra extra;

  public StatefulInvocation(List<Object> args, Object state, Object stateSpec, Extra extra) {
    this.args = args;
    this.state = state;
    this.stateSpec = stateSpec;
    this.extra = extra;
  }
}
