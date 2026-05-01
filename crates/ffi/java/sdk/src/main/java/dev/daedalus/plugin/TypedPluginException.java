package dev.daedalus.plugin;

public final class TypedPluginException extends RuntimeException {
  public final String code;

  public TypedPluginException(String code, String message) {
    super(message);
    this.code = code;
  }
}
