package com.zqh.storm.jd;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * 功能说明:
 * 设计一个 topology,来实现对一个句子里面的单词出现的频率进行统计。
 * 整个 topology 分为三个部分:
 * <p>
 * WordReader:数据源,负责发送单行文本记录(句子)
 * <p>
 * WordNormalizer:负责将单行文本记录(句子)切分成单词
 * <p>
 * WordCounter:负责对单词的频率进行累加
 *
 * @author 毛祥溢
 *         Email:frank@maoxiangyi.cn
 *         2013-8-26 下午 5:59:06
 */
public class TopologyMain {
    /**
     * @param args 文件路径
     */
    public static void main(String[] args) throws Exception {
        // Storm 框架支持多语言,在 JAVA 环境下创建一个拓扑,需要使用 TopologyBuilder 进行构建
        TopologyBuilder builder = new TopologyBuilder();

        /* WordReader 类,主要是将文本内容读成一行一行的模式
        * 消息源 spout 是 Storm 里面一个 topology 里面的消息生产者。
        * 一般来说消息源会从一个外部源读取数据并且向 topology 里面发出消息:tuple。
        * Spout 可以是可靠的也可以是不可靠的。
        * 如果这个 tuple 没有被 storm 成功处理,可靠的消息源 spouts 可以重新发射一个 tuple,但是不可靠的消息源 spouts 一旦发出一个 tuple 就不能重发了。
        *
        * 消息源可以发射多条消息流 stream。多条消息流可以理解为多中类型的数据。
        * 使用 OutputFieldsDeclarer.declareStream 来定义多个stream,然后使用 SpoutOutputCollector 来发射指定的 stream。
        *
        * Spout 类里面最重要的方法是 nextTuple。要么发射一个新的 tuple到 topology 里面或者简单的返回如果已经没有新的 tuple。
        * 要注意的是 nextTuple 方法不能阻塞,因为 storm 在同一个线程上面调用所有消息源 spout 的方法。
        *
        * 另外两个比较重要的 spout 方法是 ack 和 fail。storm 在检测到一个 tuple 被整个 topology 成功处理的时候调用 ack,否则调用 fail。storm只对可靠的 spout 调用 ack 和 fail。
        */
        builder.setSpout("word-reader", new WordReader());
        /* WordNormalizer 类,主要是将一行一行的文本内容切割成单词
        *
        * 所有的消息处理逻辑被封装在 bolts 里面。Bolts 可以做很多事情:过滤,聚合,查询数据库等等。
        * Bolts 可以简单的做消息流的传递。复杂的消息流处理往往需要很多步骤,从而也就需要经过很多 bolts。
        * 比如算出一堆图片里面被转发最多的图片就至少需要两步:
        * 第一步算出每个图片的转发数量。
        * 第二步找出转发最多的前 10 个图片。(如果要把这个过程做得更具有扩展性那么可能需要更多的步骤)。
        *
        * Bolts 可以发射多条消息流, 使用OutputFieldsDeclarer.declareStream 定义 stream,使用OutputCollector.emit 来选择要发射的 stream。
        * Bolts 的主要方法是 execute, 它以一个 tuple 作为输入,bolts使用 OutputCollector 来发射 tuple。
        * bolts 必须要为它处理的每一个 tuple 调用 OutputCollector 的ack 方法,以通知 Storm 这个 tuple 被处理完成了,从而通知这个 tuple 的发射者 spouts。
        * 一般的流程是: bolts 处理一个输入 tuple, 发射 0 个或者多个tuple, 然后调用 ack 通知 storm 自己已经处理过这个 tuple 了。storm 提供了一个 IBasicBolt 会自动调用 ack。
        */
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        /*
        * 上面的代码和下面的代码中都设定了数据分配的策略 streamgrouping
        * 定义一个 topology 的其中一步是定义每个 bolt 接收什么样的流作为输入。stream grouping 就是用来定义一个 stream 应该如果分配数据给bolts 上面的多个 tasks。
        * Storm 里面有 7 种类型的 stream grouping
        * Shuffle Grouping: 随机分组, 随机派发 stream里面的 tuple,保证每个 bolt 接收到的 tuple 数目大致相同。
        * Fields Grouping:按字段分组, 比如按 userid来分组, 具有同样 userid 的 tuple 会被分到相同的 Bolts 里的一个 task,
        * 而不同的 userid 则会被分配到不同的 bolts里的 task。
        * All Grouping:广播发送,对于每一个 tuple,所有的 bolts 都会收到。
        * Global Grouping:全局分组, 这个 tuple 被分配到 storm 中的一个 bolt 的其中一个 task。再具体一点就是分配给 id 值最低的那个 task。
        * Non Grouping:不分组,这 stream grouping 个分组的意思是说 stream 不关心到底谁会收到它的 tuple。
        * 目前这种分组和 Shuffle grouping 是一样的效果, 有一点不同的是 storm 会把这个 bolt 放到这个 bolt 的订阅者同一个线程里面去执行。
        * Direct Grouping: 直接分组, 这是一种比较特别的分组方法,用这种分组意味着消息的发送者指定由消息接收者的哪个 task 处理这个消息。
        * 只有被声明为 Direct Stream 的消息流可以声明这种分组方法。而且这种消息 tuple 必须使用 emitDirect 方法来发射。
        * 消息处理者可以通过 TopologyContext 来获取处理它的消息的 task 的 id (OutputCollector.emit 方法也会返回 task的 id)。
        * Local or shuffle grouping:如果目标 bolt 有一个或者多个 task 在同一个工作进程中,tuple 将会被随机发生给这些 tasks。
        * 否则,和普通的 Shuffle Grouping 行为一致。
        */
        builder.setBolt("word-counter", new WordCounter(), 1)
                .fieldsGrouping("word-normalizer", new Fields("word"));
        /*
        * storm 的运行有两种模式: 本地模式和分布式模式.
        * 1) 本地模式:
        * storm 用一个进程里面的线程来模拟所有的 spout 和bolt. 本地模式对开发和测试来说比较有用。
        * 你运行 storm-starter 里面的 topology 的时候它们就是以本地模式运行的, 你可以看到 topology 里面的每一个组件在发射什么消息。
        * 2) 分布式模式:
        * storm 由一堆机器组成。当你提交 topology 给master 的时候, 你同时也把 topology 的代码提交了。
        * master 负责分发你的代码并且负责给你的 topolgoy分配工作进程。如果一个工作进程挂掉了, master 节点会把认为重新分配到其它节点。
        * 下面是以本地模式运行的代码:
        *
        * Conf 对象可以配置很多东西, 下面两个是最常见的:
        * TOPOLOGY_WORKERS(setNumWorkers) 定义你希望集群分配多少个工作进程给你来执行这个 topology.
        * topology 里面的每个组件会被需要线程来执行。每个组件到底用多少个线程是通过 setBolt 和 setSpout 来指定的。
        * 这些线程都运行在工作进程里面. 每一个工作进程包含一些节点的一些工作线程。
        * 比如, 如果你指定 300 个线程,60 个进程,那么每个工作进程里面要执行 6 个线程, 而这 6 个线程可能属于不同的组件(Spout, Bolt)。
        * 你可以通过调整每个组件的并行度以及这些线程所在的进程数量来调整 topology 的性能。
        * TOPOLOGY_DEBUG(setDebug), 当它被设置成 true的话, storm 会记录下每个组件所发射的每条消息。
        * 这在本地环境调试 topology 很有用, 但是在线上这么做的话会影响性能的。
        */
        Config conf = new Config();

        conf.setDebug(true);
        conf.setNumWorkers(2);
        conf.put("wordsFile", "/root/workspace1/com.jd.storm.demo/src/main/resources/words.txt");
        conf.setDebug(true);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        /*
        * 定义一个 LocalCluster 对象来定义一个进程内的集群。提交topology 给这个虚拟的集群和提交 topology 给分布式集群是一样的。
        * 通过调用 submitTopology 方法来提交 topology, 它接受三个参数:要运行的 topology 的名字,一个配置对象以及要运行的 topology 本身。
        * topology 的名字是用来唯一区别一个 topology 的,这样你然后可以用这个名字来杀死这个 topology 的。前面已经说过了, 你必须显式的杀掉一个 topology, 否则它会一直运行。
        */
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCounterTopology", conf, builder.createTopology());
        Thread.sleep(1000);
        cluster.killTopology("wordCounterTopology");
        cluster.shutdown();
    }
}