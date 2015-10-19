package com.github.miguelantonio;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

public class EsperBoltStaticTest {

    /**
     * Test of checkEPLSyntax method, of class EsperBolt.
     */
    @Test
    public void testEPLCheck() {
        String epl = "insert into Result "
                + "select avg(price) as avg, price from "
                + "quotes_default(symbol='A').win:length(2) "
                + "having avg(price) > 60.0";
        try {
            EsperBolt.checkEPLSyntax(epl);
        } catch (EsperBoltException ex) {
            fail(ex.getMessage());
        }
    }

}