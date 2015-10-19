Storm starter-RollingTopWords: http://www.cnblogs.com/fxjwind/archive/2013/05/22/3093018.html
Storm中实现实时主题趋势: http://www.michael-noll.com/blog/2013/01/18/implementing-real-time-trending-topics-in-storm/
你了解实时计算吗? http://www.cnblogs.com/foreach-break/p/what-is-real-time-computing-and-how.html
http://www.javali.org/bigdata/storm-sliding-window-used-to-batch-compution.html

https://groups.google.com/forum/#!topic/cascading-user/KTKHwu_8t5o

怎么实现滑动窗口?
Do a self-group (via GroupBy) using whatever grouping makes sense, sorted by your timestamp.
Then implement a custom Buffer that implements the sliding window.
1. 自关联,并根据时间字段排序
2. 实现一个自定义缓冲区, 实现滑动窗口.

cn.fraudmetrix.dataplatform.koudai.KoudaiMetrics就是用自关联实现的.

