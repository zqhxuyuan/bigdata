package com.zqh.zookeeper.lock;

import junit.framework.TestCase;

import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

public class ZNodeNameTest extends TestCase {
    @Test
    public void testOrderWithSamePrefix() throws Exception {
        String[] names = { "x-3", "x-5", "x-11", "x-1" };
        String[] expected = { "x-1", "x-3", "x-5", "x-11" };
        assertOrderedNodeNames(names, expected);
    }
    @Test
    public void testOrderWithDifferentPrefixes() throws Exception {
        String[] names = { "r-3", "r-2", "r-1", "w-2", "w-1" };
        String[] expected = { "r-1", "r-2", "r-3", "w-1", "w-2" };
        assertOrderedNodeNames(names, expected);
    }

    protected void assertOrderedNodeNames(String[] names, String[] expected) {
        int size = names.length;
        assertEquals("The two arrays should be the same size!", names.length, expected.length);
        SortedSet<ZNodeName> nodeNames = new TreeSet<ZNodeName>();
        for (String name : names) {
            nodeNames.add(new ZNodeName(name));
        }

        int index = 0;
        for (ZNodeName nodeName : nodeNames) {
            String name = nodeName.getName();
            assertEquals("Node " + index, expected[index++], name);
        }
    }

}