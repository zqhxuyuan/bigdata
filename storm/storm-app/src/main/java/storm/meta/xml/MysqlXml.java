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
 * @version 2014年11月9日 上午11:26:29
 */

public class MysqlXml {
	// xml路径
	private String fd;
	// Mysql参数
	// mysql地址及端口
	public static String Host_port;
	// 数据库名
	public static String Database;
	// 数据库名
	public static String From;
	// 用户名
	public static String Username;
	// 密码
	public static String Password;

	public MysqlXml(String str) {
		this.fd = str;
	}

	public void read() {
		try {
			File file = new File(this.fd);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(file);

			NodeList nl = doc.getElementsByTagName(MacroDef.Parameter);

			Element e = (Element) nl.item(0);

			Host_port = e.getElementsByTagName(MacroDef.Host_port).item(0)
					.getFirstChild().getNodeValue();
			Database = e.getElementsByTagName(MacroDef.Database).item(0)
					.getFirstChild().getNodeValue();
			Username = e.getElementsByTagName(MacroDef.Username).item(0)
					.getFirstChild().getNodeValue();
			Password = e.getElementsByTagName(MacroDef.Password).item(0)
					.getFirstChild().getNodeValue();
			From = e.getElementsByTagName(MacroDef.From).item(0)
					.getFirstChild().getNodeValue();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
