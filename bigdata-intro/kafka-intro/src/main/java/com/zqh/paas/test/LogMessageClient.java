package com.zqh.paas.test;

import org.apache.log4j.Logger;

/**
 * Created by zqhxuyuan on 15-3-9.
 */
public class LogMessageClient {

    private static final Logger log = Logger.getLogger(LogMessageClient.class);

    public static void main(String[] args) {
        log.debug("Level01: DEBUG! ONLY OPEN IN DEVELOPMENT ENV!");
        log.info("Level02: INFO! YOU SHOULD NOTICE THIS MSG!");
        log.warn("Level03: WARNING! DANGER OPERATION");
        log.error("Level04: ERROR! SYSTEM OCCUR PROBLEMS! PLZ CONTACT ADMINISTRATOR!");
        System.exit(0);
    }
}
