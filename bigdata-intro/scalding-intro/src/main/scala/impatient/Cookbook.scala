package impatient

import com.twitter.scalding._

/**
 * Created by zhengqh on 15/9/20.
 * https://github.com/willf/scalding_cookbook
 */
class Cookbook {


}

class WordCountJob(args : Args) extends Job(args) {
  TextLine(args("input"))
    .flatMap('line -> 'word) { line : String => line.split("\\s+") }
    .groupBy('word) { _.size }
    .write(Tsv(args("output")))
}

class WordCountJob2(args : Args) extends Job(args) {
  TextLine( args("input") )
    .flatMap('line -> 'word) { line : String => tokenize(line) }
    .groupBy('word) { _.size }
    .write( Tsv( args("output") ) )

  def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}

// input (tsv)
// 0   1     2     3    4   5   6
// 22  kinds of    love nn2 io  nn1
// 12  large green eyes jj  jj  nn2
//
// output (tsv)
// 22 of    kinds/nn2_love/nn1
// 12 green large/jj_eyes/nn2

class contextCountJob(args : Args) extends Job(args) {
  val inSchema = ('count, 'w1 ,'w2, 'w3, 'pos1, 'pos2, 'pos3)
  val outSchema = ('count, 'word, 'context)
  Tsv(args("input"),inSchema)
    .mapTo(inSchema -> outSchema) { parts : (String, String, String, String, String, String, String) => {
    val (count, w1, w2, w3, pos1, pos2, pos3) = parts
    val context = "%s/%s_%s/%s".format(w1,pos1,w3,pos3)
    (count, w2, context)
  }
  }
    .write(Tsv(args("output")))
}