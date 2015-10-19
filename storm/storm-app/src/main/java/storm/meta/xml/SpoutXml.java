package storm.meta.xml;

import java.io.File;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import storm.meta.base.MacroDef;

/**
 * @author blogchong
 * @Blog www.blogchong.com
 * @email blogchong@gmail.com
 * @QQ_G 191321336
 * @version 2014年11月15日 上午22:43:11
 */

public class SpoutXml {
	// xml路径
	private static String fd;
	// MetaBolt参数
	public static String MetaRevTopic; // !--MetaQ消息队列--
	public static String MetaZkConnect; // !--MetaQ服务地址--
	public static String MetaZkRoot; // !--MetaQ服务路径--
	public static String MetaConsumerConf; // !--MetaQ消费者组ID--

	@SuppressWarnings("static-access")
	public SpoutXml(String str) {
		this.fd = str;
	}

	@SuppressWarnings("static-access")
	public void read() {
		try {
			File file = new File(this.fd);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(file);

			NodeList nl = doc.getElementsByTagName(MacroDef.Parameter);

			Element e = (Element) nl.item(0);

			MetaRevTopic = e.getElementsByTagName(MacroDef.MetaRevTopic).item(0)
					.getFirstChild().getNodeValue();
			
			MetaZkConnect = e.getElementsByTagName(MacroDef.MetaZkConnect).item(0)
					.getFirstChild().getNodeValue();
			
			MetaZkRoot = e.getElementsByTagName(MacroDef.MetaZkRoot).item(0)
					.getFirstChild().getNodeValue();
			
			MetaConsumerConf = e.getElementsByTagName(MacroDef.MetaConsumerConf)
					.item(0).getFirstChild().getNodeValue();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
