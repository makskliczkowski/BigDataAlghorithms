// WORD MINING IN BOOKS - MAKSYMILIAN KLICZKOWSKI - 236490

import scala.io.Source
import java.io._
import java.io.File
import scala.math._


object WordCloud {

  //------------GLOBAL VARIABLES---------------------
  val stopWords : Array[String] = getStopWords("stopwords_en.txt")
  val booksDir = "books"
  val booksDirLen: Int = booksDir.length()
  val printMap = "map.txt"
  //-------------HELPING FUNCTIONS-------------------
  def logBaseB(x : Double , base : Double) : Double = {
    log10(x)/log10(base)
  }
  def getStopWords(dir : String): Array[String] = {
    val stopWordsFile = Source.fromFile(dir, "UTF-8")
    val stopWords = stopWordsFile.mkString.split( "\\s+")
    stopWordsFile.close()
    stopWords
  }
  // List files in given directory
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
  // check StopWords
  def notInStopWords(w: String): Boolean = {
    ! stopWords.contains(w)
  }
  // Map the book to a sequence from file in UTF-8 format
  def textMapper(fn: String): Seq[(String,Int)] = {
    val buffer  = Source.fromFile(fn, "UTF-8")
    val book = buffer.mkString.toLowerCase.replaceAll("['\"#_,“.”!-:?*;»…()«]", "").split("\\s+")
      .filterNot(stopWords.contains(_))
      .groupBy(identity).view.mapValues(_.length)
      .toSeq
      .sortWith((x,y)=>x._2>y._2)
      .distinct
    buffer.close()
    book
  }
  // Print the sequence
  def sequencePrinter(sequence : Seq[(String,Int)], fn : String): Unit ={
    val printer  = new PrintWriter(new File(fn),"UTF-8")
    for (x<-sequence){
      printer.write(x._1+";"+x._2.toString+"\n")
    }
    printer.flush()
    printer.close()
  }
  // Print all to file
  def printBooks(booksMap : Map[String,Seq[(String, Double)]],number : Map[String,Int]) : Unit = {
    val printer  = new PrintWriter(new File(printMap),"UTF-8")
    for (book <- number){
      printer.write("----------"+book._1+"---------- : total distinct words : " + book._2 + '\n')
      for (seq <- booksMap(book._1)){
        printer.write(seq.toString() + '\n' )
      }
    }
    printer.flush()
    printer.close()
  }
  // Create a map of sequences of tuples for each book
  def eachBook(bookList : List[String]): Map[String,Seq[(String,Double)]] ={
    var forAll = collection.mutable.Map[String,Seq[(String,Double)]]()
    for (book <- bookList){
      val sequence = textMapper(book)
      // because the sequence is sorted
      val max = sequence.head._2.toDouble
      val newSequence  = sequence.map(x => Tuple2(x._1,x._2/max):(String,Double))
      forAll += (book.reverse.dropRight(booksDirLen+1).reverse -> newSequence)
    }
    forAll.toMap
  }
  def getNumberOfDistinctWords(booksMap : Map[String,Seq[(String,Double)]]) : Map[String , Int] = {
    var map = collection.mutable.Map[String, Int]()
    booksMap foreach {
      case (key, value) =>
        val k = key
        val v = value.length
        map += (k ->v)
    }
    map.toMap
  }
  //here we create set of all possible words and then count how many times they are there
  def howManyInOtherBooks(booksMap : Map[String,Seq[(String, Double)]]) : Set[(String,Int)] = {
    var set = scala.collection.mutable.Set[String]()
    booksMap foreach { // create a set
      case (_, sequence) =>
        set ++= sequence.map(x => x._1).toSet
    }
    set.groupBy(identity)
      .view.mapValues(_.size)
      .toSet
  }
  def getMapWithTFIDF(booksMap : Map[String,Seq[(String, Double)]]) : Map[String,Seq[(String,Double)]] ={
    val howManyBooks = booksMap.size
    var returner = collection.mutable.Map[String,Seq[(String,Double)]]()
    //check for each word
    val setOfAllWords = howManyInOtherBooks(booksMap)
    booksMap foreach {
      case (book, sequence) =>
        // create TF_ijxIDF_i for each word and place it in sequence again
        val newSequence  = sequence.map(x => Tuple2(x._1,x._2*logBaseB(howManyBooks.toFloat/setOfAllWords.find(word => word._1 == x._1).get._2.toFloat,2)))
          // we are sure that the word must be in that list because it's how we created it, so an Option is not a problem and we get do .get!
          .sortWith((x,y)=>x._2>y._2)
        returner += (book -> newSequence)
        println("Created map for " + book + '\n')
    }
    returner.toMap
  }
  // function that gives List of matching books for given word according to TFIDF coefficient
  def giveWordIGiveYouBooks(word : String, mapWithTFIDF : Map[String,Seq[(String,Double)]])  : List[(String,Double)] = {
    var list = collection.mutable.ListBuffer[(String,Double)]()
    mapWithTFIDF foreach{
        case (book, sequence) =>
            try {
                val found = sequence.find(x => x._1 == word.toLowerCase())
                val tuple: (String, Double) = (book,found.get._2)
                list += tuple
            } catch {
              case e: Exception =>
                println(e,'\t')
                println("Haven't found " + word + " in " + book + " giving -1 instead"+'\n')
                val found : (String,Double) = (book, -1.0)
                list += found
            }
    }
    list.sortWith((x,y) => x._2 >= y._2).toList
  }

  def main(args: Array[String]) {
    val files = getListOfFiles(booksDir).map(_.toString)
    //println(files)
    //val cutFiles = files.map(_.reverse.dropRight(booksDirLen+1).reverse)
    //println(cutFiles)
    val bookList = eachBook(files)
    val numberInEach = getNumberOfDistinctWords(bookList)
    val mapWithTFIDF = getMapWithTFIDF(bookList)
    //println(bookList)
    //TELL ABOUT BOOK LENGTH AND AUTHORS
    var mapWithTFIDF_20 = collection.mutable.Map[String,Seq[(String,Double)]]()
    // cut to 20 only
    mapWithTFIDF foreach{
      case (book, sequence) =>
        mapWithTFIDF_20 += book -> sequence.slice(0,20)
    }
    printBooks(mapWithTFIDF_20.toMap,numberInEach)
    // generate list of random words f.e https://randomwordgenerator.com/
    val listOfWords = Seq("neutral", "coffin","large" ,"matter","rest", "theory" ,"normal","indoor","basket","tablet")
    for ( word <- listOfWords ){
      println(word,'\n', giveWordIGiveYouBooks(word,mapWithTFIDF),'\n')
    }

  }

}
