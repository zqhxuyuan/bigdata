package storm.kafka.demo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class WordCounter implements IRichBolt{
    private static final long serialVersionUID = 1L;
	Log LOG=LogFactory.getLog(WordCounter.class);

	Integer id;
	String name;
	Map<String,Integer> counters;
	private OutputCollector collector;

    static Connection conn; // 创建静态全局变量
    static Statement st;
    
    /* 获取数据库连接的函数*/  
    public static Connection getConnection() {  
        Connection con = null;  //创建用于连接数据库的Connection对象  
        try {  
            Class.forName("com.mysql.jdbc.Driver");// 加载Mysql数据驱动  
            con = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/test", "root", "root");// 创建数据连接
        } catch (Exception e) {
            System.out.println("数据库连接失败" + e.getMessage());  
        }  
        return con; //返回所建立的数据库连接
    }  
    
	public static void insert(String word,int value) {
        conn = getConnection(); // 首先要获取连接，即连接到数据库
        try {
            String sql = "INSERT INTO words(word,count) VALUES ('"+word+"','"+value+"')";  // 插入数据的sql语句
            st = (Statement) conn.createStatement();    // 创建用于执行静态sql语句的Statement对象
            int count = st.executeUpdate(sql);  // 执行插入操作的sql语句，并返回插入数据的个数
            System.out.println("向words表中插入 " + count + " 条数据"); //输出插入操作的处理结果
            conn.close();   //关闭数据库连接
        } catch (SQLException e) {
            System.out.println("插入数据失败" + e.getMessage());  
        }
    }
    
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counters=new HashMap<String, Integer>();
		this.collector=collector;
		this.name=context.getThisComponentId();
		this.id=context.getThisTaskId();
	}

	public void execute(Tuple input) {
		String str=input.getString(0);
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		}else{
			Integer c=counters.get(str)+1;
			counters.put(str, c);
		}
		for (Map.Entry<String, Integer> entry:counters.entrySet()) {
			// insert(entry.getKey(),entry.getValue());
            System.out.println(entry.getKey() + ": " + entry.getValue());
		}
		collector.ack(input);
	}

	
	public void cleanup() {
		for (Map.Entry<String, Integer> entry:counters.entrySet()) {
			// insert(entry.getKey(),entry.getValue());
            System.out.println("word: [" + entry.getKey() + "], count: [" + entry.getValue() + "]");
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
