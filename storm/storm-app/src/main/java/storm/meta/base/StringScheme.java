package storm.meta.base;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author blogchong
 * @Blog   www.blogchong.com
 * @email  blogchong@gmail.com
 * @QQ_G   191321336
 * @version 2014年11月15日 上午21:36:49
 */

//规范化输出
@SuppressWarnings("serial")
public class StringScheme implements Scheme {

	public List<Object> deserialize(byte[] bytes) {

		try {
			//数据的编码定义
			return new Values(new String(bytes, MacroDef.ENCODING));

		} catch (UnsupportedEncodingException e) {

			throw new RuntimeException(e);

		}
	}

	public Fields getOutputFields() {
		return new Fields("str");
	}
}