package com.hadoop;

import org.junit.Test;

public class WordCountTest {

    @Test
    public void testWordCounterUpload(){
        System.out.println("Starting WordCounter uploader Test");
        String[] args = new String[]{"/home/rahul/Rahul/solveitinjava/words.csv"};
        WordCountDriver.main(args);

        System.out.println("WordCounter Test Completed.");
    }
}