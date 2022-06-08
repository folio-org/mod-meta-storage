package org.folio.metastorage.server;

import org.folio.okapi.testing.UtilityClassTester;
import org.junit.Test;

public class OaiPmhClientTest {

  @Test
  public void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(OaiPmhClient.class);
  }

}
