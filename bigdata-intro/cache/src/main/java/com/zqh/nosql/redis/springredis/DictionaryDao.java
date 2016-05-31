package com.zqh.nosql.redis.springredis;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

@Repository
public class DictionaryDao {

	private static final String ALL_UNIQUE_WORDS = "all-unique-words";
	
	private StringRedisTemplate redisTemplate;

	@Inject
	public DictionaryDao(StringRedisTemplate redisTemplate){
		this.redisTemplate = redisTemplate;
	}
	
	public Long addWordWithItsMeaningToDictionary(String word, String meaning) {
		Long index = redisTemplate.opsForList().rightPush(word, meaning);
		redisTemplate.opsForSet().add(ALL_UNIQUE_WORDS, word);
		return index;
	}

	public List<String> getAllTheMeaningsForAWord(String word) {
		List<String> meanings = redisTemplate.opsForList().range(word, 0, -1);
		return meanings;
	}

	public void removeWord(String word) {
		redisTemplate.delete(Arrays.asList(word));
	}

	public void removeWords(String... words) {
		redisTemplate.delete(Arrays.asList(words));
	}

	public Set<String> allUniqueWordsInDictionary() {
		Set<String> allUniqueWords = redisTemplate.opsForSet().members(
				ALL_UNIQUE_WORDS);
		return allUniqueWords;
	}

	public int countOfAllUniqueWords() {
		Set<String> allUniqueWords = redisTemplate.opsForSet().members(
				ALL_UNIQUE_WORDS);
		return allUniqueWords.size();
	}

	public WordMeaningPair randomWord() {
		String randomWord = redisTemplate.opsForSet().randomMember(ALL_UNIQUE_WORDS);
		List<String> meanings = redisTemplate.opsForList().range(randomWord, 0, -1);
		WordMeaningPair wordMeaningPair = new WordMeaningPair(randomWord,
				meanings);
		return wordMeaningPair;
	}

	public Long countOfMembersInASet(String key) {
		return redisTemplate.opsForSet().size(key);
	}

}
