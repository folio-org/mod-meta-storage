package org.folio.metastorage.util;

import java.util.regex.Pattern;

public class SourceId {

  private static final String PATTERN_STRING = "^[a-zA-Z0-9:/-]{1,16}$";
  private static final Pattern PATTERN_COMPILED = Pattern.compile(PATTERN_STRING);

  final String sourceId;

  /**
   * Construct source identifier.
   * @param s source identifier string
   * @throws IllegalArgumentException if not according to pattern
   */
  public SourceId(String s) {
    if (!PATTERN_COMPILED.matcher(s).find()) {
      throw new IllegalArgumentException(s + " does not match " + PATTERN_STRING);
    }
    this.sourceId = s.toUpperCase();
  }

  public String toString() {
    return sourceId;
  }
}
