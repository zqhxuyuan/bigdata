package com.zqh.nosql.redis.springredis;
import java.util.ArrayList;
import java.util.List;

public class WordMeaningPair {

    private final String word;
    private final List<String> meanings;

    public WordMeaningPair(String word, List<String> meanings) {
        this.word = word;
        this.meanings = new ArrayList<String>(meanings);
    }

    public String getWord() {
        return word;
    }

    public List<String> getMeanings() {
        return new ArrayList<String>(meanings);
    }

    @Override
    public String toString() {
        return "WordMeaningPair [word=" + word + ", meanings=" + meanings + "]";
    }
}