trident-examples
================

A set of applications written in Storm Trident.


## Applications

 - [Sentiment Analysis](src/main/java/storm/trident/topology/SentimentAnalysis.java)
 - [Trending Topics](src/main/java/storm/trident/topology/TrendingTopics.java)
 - [Word Count](src/main/java/storm/trident/topology/WordCount.java)
 
## Usage

### Build

```bash
$ git clone git@github.com:mayconbordin/trident-examples.git
$ cd trident-examples/
$ mvn -P<profile> package
```

Use the `local` profile to run the applications in local mode or `cluster` to run in a remote cluster.


### Submit a Topology

Syntax:

```bash
$ storm <jar> storm.trident.StormRunner --app <application-name> --mode (local|remote) [OTHER OPTIONS...]
```

Options:

```bash
  --config-str=<str>    A serialized list of key/value pairs (ex.: sa.spout.threads=2,sa.sink.threads=5)
  --runtime=<runtime>    Runtime in seconds (local mode only) [default: 300].
  --topology-name=<name> The name of the topology (remote mode only).
```
