
## 业务逻辑

因为实际的业务逻辑是: 客户端请求, 引擎返回结果给客户端, 然后写入到后台的Kafka消息队列中.
实时指标计算从Kafka中中获取. 那么这个数据就不是客户端实时传过来的数据了!
那么实时指标计算能做的是, 对于这些历史数据, 计算出指标, 下次客户端再次传递数据进来时, 直接使用指标值.

```

Request --> RiskEngine --> Velocity --> Result

```

1.Topology如何动态定义时间片:

按照RollingTopWords定义的RollingCountBolt, 指定了时间窗口. 但是我们的业务系统的规则不一样:
一条事件对应一个合作方,一个合作方有很多指标,而每个指标定义的时间窗口都不一样.
如果是根据不同的时间窗口长度,定义不同的Topology,这样的Topology数量就很多.不可取!

问题: 所以由于时间片不一样, 不能在Topology中写死, 但是RollingCountBolt需要时间片,怎么动态地传进来?
解决: 可以在一个Topology中定义多个Bolt,参数为不同的滑动时间窗口!

2.JSONBolt:

根据不同的计算类型, 做不同的聚合操作. 但是如何在Bolt内部完成呢, 一般是通过在Topology中配置不同的Source来指向不同的Bolt?
这里可以通过stream-id. 不过需要在Topology中设置对应的Bolt用指定的stream-id接收数据.
比如这里发射的stream-id=COUNT, 则CountBolt配置的分组策略的第二个参数stream-id=COUNT.
则CountBolt就会接收到这里发射的Tuple数据, 其他Bolt不会接收到的! stream-id可以认为是一种分流策略.

存在的问题是: 一条事件, 因为有很多指标, 发射的tuple数据量就会被放大. 当然放大的因子只是计算类型的数量,而不是指标的数量.
TODO 因为不同的指标都有不同的时间窗口, 能不能把时间窗口也加入到分流stream-id策略中??

3.TimedCountBolt

上面1中需要动态传入时间窗口的问题目前的解决办法是:
定义一些常用的时间窗口, 让Bolt在初始化时(比如在prepare方法中)创建所有的SlidingWindowCounter对象.
这样的缺点是如果页面中自定义了一个我们这里没有定义的时间窗口,则无法创建对应的Counter.

因为JSONBolt对每条事件记录,分别传递所有这条事件对应的指标. 这样TimedCountBolt接收到的分别是:一条事件记录,其中的一个指标.
这样在execute方法中根据每个不同的指标对应的时间窗口长度, 获得对应的Counter, 只在对应的Counter上把tuple放入对应的时间窗口中.
SlidingWindowCounter<Object> counter = counterMap.get(timeUnit);

当时间窗口满足时, 发射数据:
不同的Counter代表不同的时间窗口. 比如一条记录对应的指标有: IP分别在一天,7天,一个月出现的次数.
因为一开始我们对不同的时间窗口, 都放在了不同的SlidingWindowCounter中. 所以输出结果也是针对不同的时间窗口.

TODO 对于每种时间窗口, 都使用统一的发射频率(比如10s). 对于3个月这么长的时间窗口, 需要自定义吗? 或者说针对不同长度的时间窗口, 定义不同的发射频率?

**如何设计发射出去的值**

使用Counter增加计数器的值. 这里发射的值要进行区分. 假设要同时统计一天内的ip数,账户数,设备数.
如果数据源仅仅是传一个ip值,账户值,设备值,到emit时,无法区分出这个值到底是ip,还是账户,设备.
同样,还要根据合作方,应用名称,事件类型进行区分. 也就是数据源发射的数据的设计要能够体现出业务的意义.

所以对于统计主维度出现的次数, 设计的发射到Counter的key:

```
String key = tdMetric.getKey() +
                timeUnit + WindowConstant.splitKey +
                tdMetric.getMasterField() + WindowConstant.splitKey +
                tdMetric.getMasterValue();
```

上面这样不同合作方的事件记录共用同一个Counter, 会不会有问题? 同样涉及到数据源的问题.确保计算的数据能体现业务逻辑.


4.TimedDistinctCountBolt

```
主维度名称::从维度名称::主维度值::从维度值   count
IP::ACC::1.1.1.1::AA                    2
IP::ACC::1.1.1.1::BB                    3

主维度名称::从维度名称::主维度值            去重的从维度个数
IP::ACC::1.1.1.1                        2
```

5.分组策略fieldsGrouping

RollingTopWords中的RollingCountBolt使用的是fieldsGrouping("word")
即统计单词出现的次数(对应我们的是主维度出现的次数)要使用字段分组. 更不用说主维度关联的从维度个数.


### 滑动窗口演示

滑动时间为1秒,时间窗口为4秒,一共有4个slot.

```
     1 to 4                4 end,clearSlot:1         5 come in(u3)       5 end               6 come in(non events)
     -----------             -----------             -----------         -----------         -----------
[4] | u9  | u9  | [1]       | u9  | ×   |           | u9  | u3  |       | u9  | u3  |       | u9  | u3  |
    |     | u2  |           |     |     |           |     |     |       |     |     |       |     |     |
     -----------             -----------             -----------         -----------         -----------
[3] | u4  | u3  | [2]       | u4  | u3  |           | u4  | u3  |       | u4  | ×   |       | u4  |     |
    |     |     |           |     |     |           |     |     |       |     |     |       |     |     |
     -----------             -----------             -----------         -----------         -----------
1:u9:1,u2:1                                         5:u9:1,u3:2,u4:1                        6:u9:1,u3:1,u4:1
2:u9:1,u2:1,u3:1                 ⬆                        |                   ⬆
3:u9:1,u2:1,u3:1,u4:1            ⬆                        |--------------------
4:u9:2,u2:1,u3:1,u4:1 ------------
```

### 滑动窗口内没有事件出现,也会重复输出

TODO 因此要考虑一个问题, 之前的数据, 在本次20秒的滑动窗口之内都没有再次出现,是否有必要再次输出??
以业务系统时间窗口为一个月为例, 假设用户平均一周登陆一次, 而这里滑动窗口为20秒. 一个月的slot=30*24*3600/20=30*24*180=129600
(假设滑动窗口缩小为5秒, 则slots=129600*4=518400, 再假设时间窗口为3个月, slots=518400*3=1555200=155万!)

用户在第一次滑动窗口出现时, 在接下来的所有滑动窗口中, 即使都不再出现, 但是由于都还在时间窗口之内
(即虽然不在当前slot中,但是在时间窗口的所有slot允许的之前的slot中), 统计的时候仍然会一起被输出!

```
         slot1 .. slot4
 -----------   |     第一个滑动窗口: slot1                         => u9:1, u2:1
| u9  | u9  |  |     第二个滑动窗口: slot2 + slot1                 => u9:1, u2:1, u3:1
|     | u2  |  |     第三个滑动窗口: slot3 + slot2 + slot1         => u9:1, u2:1, u3:1, u4:1
 -----------   |     第四个滑动窗口: slot4 + slot3 + slot2 + slot1 => u9:2, u2:1, u3:1, u4:1
| u4  | u3  |  |
|     |     |  |     可以看到u9自在第一个slot中出现后, 在很长一段滑动窗口中都没有再次出现(假设slot更多的话),
 -----------   |     但是每次滑动时,都会再次输出u9的结果, 而这个值由于没有新事件进来,count值都没有发生变化.
               |
 <-------------|
```

### 很大的数组只有几个slot被用到

考虑极值的情况: 最小滑动窗口=1秒, 最长时间窗口=6个月, slots=15,552,000=1500万!
SlotBasedCounter的Map<T, long[]> objToCounts的含义是key在所有slots中的分布.
key本身代表的是业务数据:指标(比如某个合作方的账户AA, 不是具体值比如账户数出现了5次), 这个数据本身量就很大!
现在对于其中一个key而言, long[]代表的是所有slot的值, 这个数组最大有1500万个元素!

使用数组存放指标在某个slot中出现的次数的缺点是: 如果某些slot没有出现这个指标对应的事件, 则这些slot为空值.
这样尽管slot数组很大, 但是可能只有几个slot内有值! 也就是说分配的这么多个slot,实际用的slot没几个!

能想到的就是使用别的数据结构来代替数组. 而且这个数据结构首先能表示count值. 首先用List替换.
首先有个所有数据结构必须面对的问题, 在incrementCount中,如果obj(key,指标)对应的数据结构不存在,
则首先创建用于存储obj的数据结构, 而这个数据结构要和slot数量关联上,
比如创建一个new long[this.numSlots]大小的数组:一开始可能就分配了很大的数组给这一个key!
所以我们要避免这样的操作(初始化一个容量很大的对象,Array或List都一样),
因为如果这个key像上面所说的只有几个slot有值,那么有很多slot都是被浪费的.

如果在初始化的时候不用到slots,那么是否有必要计算slots??
使用List的一个方案参考slidingwindow的SlidingWindowCache.

下面的T均为要保存的值,比如key. 由于Map的key不同, 所以在构造函数,添加元素,获取所有元素时存在不同.
SlotBasedCounter的key是对象本身, SlidingWinowCache则是slot编号.

```
                                                 构造函数                               increment
SlotBasedCounter:  Map<T, long[]> objToCounts    只是确定slots                          key不存在,初始化slot数组,往当前slot的count++
SlidingWinowCache: Map<Integer,List<T>> tupMap   初始化每个slot,map的value为null         添加当前T到当前slot的list中

                                                 getAndAdvanceWindow
SlotBasedCounter:  Map<T, long[]> objToCounts    循环每个key,对所有slot出现的次数求和, 最后的结果是每个key在所有slot中总的出现次数
SlidingWinowCache: Map<Integer,List<T>> tupMap   根据当前slot,往前推所有的slot, 最后的结果是所有slot中总的List<T>
```

### 指标存储

由于业务系统的数据量很大, 而单个用户的活动次数并不会很频繁(一个用户一天登陆10次就会被拦截掉, 而用户数则很多). 
尽管规则定义的时间窗口跨度可以长至3个月,6个月. 我们并不需要设置window的长度那么长, 因此需要一个中间介质来存储指标.

同样要面临一个问题: 由于只是中间介质,所以保存的应该是详细信息, 而不是指标结果.


