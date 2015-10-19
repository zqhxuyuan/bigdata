package sparkly.component.nlp

import java.util.regex.Pattern
import org.apache.lucene.analysis.br.BrazilianAnalyzer
import org.apache.lucene.analysis.el.GreekAnalyzer
import org.apache.lucene.analysis.it.ItalianAnalyzer
import org.apache.lucene.analysis.no.NorwegianAnalyzer
import org.apache.lucene.analysis.pattern.PatternReplaceFilter
import org.apache.lucene.analysis.shingle.ShingleFilter
import org.apache.lucene.analysis.standard.{UAX29URLEmailAnalyzer, StandardAnalyzer}
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.tr.TurkishAnalyzer
import org.apache.lucene.analysis.{TokenStream, Analyzer}
import org.apache.lucene.analysis.de.GermanAnalyzer
import org.apache.lucene.analysis.ar.ArabicAnalyzer
import org.apache.lucene.analysis.hi.HindiAnalyzer
import org.apache.lucene.analysis.ru.RussianAnalyzer
import org.apache.lucene.analysis.ga.IrishAnalyzer
import org.apache.lucene.analysis.fa.PersianAnalyzer
import org.apache.lucene.analysis.pt.PortugueseAnalyzer
import org.apache.lucene.analysis.cz.CzechAnalyzer
import org.apache.lucene.analysis.cjk.CJKAnalyzer
import org.apache.lucene.analysis.bg.BulgarianAnalyzer
import org.apache.lucene.analysis.fi.FinnishAnalyzer
import org.apache.lucene.analysis.sv.SwedishAnalyzer
import org.apache.lucene.analysis.da.DanishAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.fr.FrenchAnalyzer
import org.apache.lucene.analysis.gl.GalicianAnalyzer
import org.apache.lucene.analysis.hy.ArmenianAnalyzer
import org.apache.lucene.analysis.eu.BasqueAnalyzer
import org.apache.lucene.analysis.id.IndonesianAnalyzer
import org.apache.lucene.analysis.ca.CatalanAnalyzer
import org.apache.lucene.analysis.ro.RomanianAnalyzer
import org.apache.lucene.analysis.lv.LatvianAnalyzer
import org.apache.lucene.analysis.es.SpanishAnalyzer
import org.apache.lucene.analysis.ckb.SoraniAnalyzer
import org.apache.lucene.analysis.th.ThaiAnalyzer
import org.apache.lucene.analysis.hu.HungarianAnalyzer
import org.apache.lucene.analysis.util.CharArraySet

case class TextTokenizer(language: String = "", minNGram: Int = 1, maxNGram: Int = 1, ignorePattern: String = "") {

  val analyzers = new AnalyzerFactory()

  def tokenize(text: String): List[String] = {
    val stream = createTokenStream(text)
    val attribute = stream.addAttribute(classOf[CharTermAttribute])
    stream.reset

    val builder = List.newBuilder[String]
    while (stream.incrementToken()) {
      val token = attribute.toString
      if (token != null && !token.isEmpty)
        builder += attribute.toString
    }

    stream.close
    builder.result
  }

  private def createTokenStream(content: String): TokenStream = {
    val text = if (ignorePattern == null || ignorePattern.isEmpty) content else content.replaceAll(ignorePattern, "")
    val analyzer = analyzers.create(language)

    val tokenStream = analyzer.tokenStream("", text)
    (minNGram, maxNGram) match {
      case (1, 1) => tokenStream
      case _ => {
        val nGramStream = new ShingleFilter(tokenStream, minNGram, maxNGram)
        new PatternReplaceFilter(nGramStream, Pattern.compile(".*_.*"), "", true)
      }
    }
  }

}

class AnalyzerFactory extends Serializable {

  def create(language: String): Analyzer = language match {
    case "Greek" => new GreekAnalyzer()
    case "German" => new GermanAnalyzer()
    case "Arabic" => new ArabicAnalyzer()
    case "Hindi" => new HindiAnalyzer()
    case "Russian" => new RussianAnalyzer()
    case "Irish" => new IrishAnalyzer()
    case "Brazilian" => new BrazilianAnalyzer()
    case "Persian" => new PersianAnalyzer()
    case "Portuguese" => new PortugueseAnalyzer()
    case "Czech" => new CzechAnalyzer()
    case "CJK" => new CJKAnalyzer()
    case "Bulgarian" => new BulgarianAnalyzer()
    case "Finnish" => new FinnishAnalyzer()
    case "Swedish" => new SwedishAnalyzer()
    case "Danish" => new DanishAnalyzer()
    case "English" => new EnglishAnalyzer()
    case "French" => new FrenchAnalyzer()
    case "Turkish" => new TurkishAnalyzer()
    case "Email" => new UAX29URLEmailAnalyzer()
    case "Galician" => new GalicianAnalyzer()
    case "Armenian" => new ArmenianAnalyzer()
    case "Italian" => new ItalianAnalyzer()
    case "Norwegian" => new NorwegianAnalyzer()
    case "Basque" => new BasqueAnalyzer()
    case "Indonesian" => new IndonesianAnalyzer()
    case "Catalan" => new CatalanAnalyzer()
    case "Romanian" => new RomanianAnalyzer()
    case "Latvian" => new LatvianAnalyzer()
    case "Spanish" => new SpanishAnalyzer()
    case "Sorani" => new SoraniAnalyzer()
    case "Thai" => new ThaiAnalyzer()
    case "Hungarian" => new HungarianAnalyzer()
    case _ => new StandardAnalyzer(CharArraySet.EMPTY_SET)
  }
}

object AnalyzerFactory {
  val availableLanguages = List("Greek","German","Arabic","Hindi","Russian","Irish","Brazilian","Persian","Portuguese","Czech","CJK","Bulgarian","Finnish","Swedish","Danish","English","French","Turkish","Email","Galician","Armenian","Italian","Norwegian","Basque","Indonesian","Catalan","Romanian","Latvian","Spanish","Sorani","Thai","Hungarian")
}