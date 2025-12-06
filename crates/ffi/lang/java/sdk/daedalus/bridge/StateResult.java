package daedalus.bridge;

public final class StateResult {
  public final Object state;
  public final Object outputs;

  public StateResult(Object state, Object outputs) {
    this.state = state;
    this.outputs = outputs;
  }
}

