package com.zdatainc.rts.storm;

import com.zdatainc.rts.model.PositiveWords;
import org.apache.log4j.Logger;
import java.util.Set;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PositiveSentimentBolt extends BaseBasicBolt
{
    private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =
        Logger.getLogger(PositiveSentimentBolt.class);

    public void execute(Tuple input, BasicOutputCollector collector)
    {
        LOGGER.debug("Calculating positive score");
        Long id = input.getLong(input.fieldIndex("tweet_id"));
        String text = input.getString(input.fieldIndex("tweet_text"));

        //字典中预先设置的所有正能量词库
        Set<String> posWords = PositiveWords.getWords();

        //判断twitter的每个单词是否在正能量词库中
        String[] words = text.split(" ");
        int numWords = words.length;
        int numPosWords = 0;
        for (String word : words)
        {
            if (posWords.contains(word))
                numPosWords++;
        }

        //属于正能量的词站所有词的比例, 为正能量的得分值. 因此一条twitter只会有一条结果
        collector.emit(new Values(id, (float) numPosWords / numWords, text));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweet_id", "pos_score", "tweet_text"));
    }
}
