// Project on testing approximate counting methods. It will be used to count similarity between objects. The algorithms
// are based on HyperLogLog "HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm" and in case of
// documents similarity we will use hashing methods

import scala.io.Source
import java.io._
import java.io.File

import sun.security.ec.ECDSAOperations.Seed

import scala.collection.IterableOnce.iterableOnceExtensionMethods
import scala.math._
import scala.util.hashing.{MurmurHash3, byteswap32, byteswap64}
import scala.collection.mutable._


object Similarity {
  //-------------GLOBAL VARIABLES--------------------
  // HYPER_LOG_LOG
  val lbl_tcp_dir = "lbl-pkt-4/lbl-pkt-4.tcp"
  val seed_hll = 0xf7ca7fd2
  val min_b = 4
  val alphas: Array[Double] = Array(0.673, 0.697, 0.709) // for first 3 from P. Flajolet
  // READING DOCUMENTS
  val stopWords : Array[String] = getStopWords("stopwords_en.txt")
  val booksDir = "books"
  implicit def convertBoolToInt(b: Boolean) = if(b) 1 else 0
  //-------------HELPING FUNCTIONS-------------------
  // logarithm for any given base
  def logBaseB(x : Double , base : Double) : Double = {
    log10(x)/log10(base)
  }
  // Create ByteArray
  def int2Bytes(i: Int): Array[Byte] = {
    val buf = new Array[Byte](4)
    java.nio.ByteBuffer
      .wrap(buf)
      .putInt(i)
    buf
  }
  //64 bit MurMurHash3
  def MurMurHash3_64(u: String, seed: Int): Long = {
    val a = scala.util.hashing.MurmurHash3.stringHash(u, seed)
    val b = scala.util.hashing.MurmurHash3.stringHash(u.reverse, seed)
    // shift a 32 bits to the left, leaving 32 lower bits zeroed
    // mask b with "1111...1" (32 bits)
    // bitwise "OR" between both leaving a 64 bit hash of the string using Murmur32 on each portion
    val c: Long = a.toLong << 32 | (b & 0xffffffffL)
    c
  }
  // HYPER_LOG_LOG >>>>>>>>>>>>>>>
  // we give b and m is 2^b, then we assert bigger than min, from P. Flajolet
  def cal_alpha_m(b:Int) : Double= {
    assert(b >= min_b)
    if(b <= 6) {
      alphas(b-min_b)
    }
    else{
      val m = pow(2,b)
      0.7213/(1+(1.079/m))
    }
  }
  // Calculate hash and return it as String
  def countHashedArrayBits(value : String, size : Int) : String = {
    var hash = ""
    if(size == 32){
      val v = int2Bytes(byteswap64(value.toInt).toInt)
      hash = MurmurHash3.bytesHash(v, seed_hll).toBinaryString
    }
    else{
      hash = MurMurHash3_64(value,seed_hll).toBinaryString
    }
    val start_zeros = Array.fill(size - hash.length)('0').mkString

    start_zeros + hash
  }
  // Leading zeros counter in bitwise implementation. In scala int is 32 bits. FirstCrops is used to move the number if needed
  def countLeadingZerosNum32bit(a : Int, firstCrops : Int ):Int ={
    var x = a
    val temp_test = x.toBinaryString
    var n = 32
    var y = 0
    // 16 move
    y = x >> 16
    if(y != 0){
      n = n - 16
      x = y
    }
    // 8 move
    y = x >> 8
    if(y != 0){
      n = n - 8
      x = y
    }
    // 4 move
    y = x >> 4
    if(y != 0){
      n = n - 4
      x = y
    }
    // 2 move
    y = x >> 2
    if(y != 0){
      n = n - 2
      x = y
    }
    // 1 move
    y = x >> 1
    if(y != 0){
      n = n - 1
      return n-2
    }
    val returner = n-x
    if(returner == 32){
      returner - firstCrops
    }
    else returner
  }
  def countLeadingZerosNum64bit(a : Long, firstCrops : Int ):Int ={
    var x = a
    // val temp_test = x.toBinaryString
    var n = 64
    var y = 0L

    // 32 move
    y = x >> 32
    if(y != 0){
      n = n - 32
      x = y
    }

    // 16 move
    y = x >> 16
    if(y != 0){
      n = n - 16
      x = y
    }
    // 8 move
    y = x >> 8
    if(y != 0){
      n = n - 8
      x = y
    }
    // 4 move
    y = x >> 4
    if(y != 0){
      n = n - 4
      x = y
    }
    // 2 move
    y = x >> 2
    if(y != 0){
      n = n - 2
      x = y
    }
    // 1 move
    y = x >> 1
    if(y != 0){
      n = n - 1
      return n-2
    }
    val returner = n-x.toInt
    if(returner == 64){
      return returner.toInt - firstCrops
    }
    return  returner
  }
  def countLeadingZerosArray(a: String):Int ={
    var i = 0
    var temp = a(i).toString.toInt
    var zeros_number = 0
    while(temp != 1){
      i = i+1
      temp = a(i).toString.toInt
      zeros_number = zeros_number + 1
    }
    return zeros_number
  }
  //-------------- READING THE DOCUMENTS --------------------------
  // getStop words from a given directory
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
  // read file and return it as Array
  def readFileToString(dir: String) : Array[String] ={
    val buffer  = Source.fromFile(dir, "UTF-8")
    val book = buffer.mkString.toLowerCase.replaceAll("[\\[\\]'\"#_,“.”!-:?*;»…()«\t]", "").split("\\s+")
      .filterNot(stopWords.contains(_))
    book
  }
  //--------------H Y P E R LOG LOG ------------------------------
  // Implementation of raw version of hyperloglog algorithm. Here file is used as a stream source, yet it can be easily
  // extended to real streams of any kind. Here we just want to test its capabilities. Here, it will get an item from stream
  // v, which will be String, b is the power of two for the register length m[4....16], seed is a seed for MurMurHash
  // - it needs to be a constant as we need to use the same hash function, M will be the register array of size m. x will be
  // the hashed value represented by n bits
  // p is position from the right
  def hloglogRaw_step(v: String, b: Int, seed: Int, n : Int, M: Array[Int]) : Unit = {
    // WITH ARRAYS
    val x_arr = countHashedArrayBits(v,n)
    var temp_j = x_arr.take(b)
    val j = Integer.parseInt(temp_j,2)
    val w = x_arr.takeRight(n-(b))
    val q = countLeadingZerosArray(w)
    // val x = abs(MurmurHash3.stringHash(v.toInt.toBinaryString,seed))
    //val test = x.toBinaryString
    // get first b elements by setting b bits to one and AND it with right moved number by p = n-b -1  positions
    //var j = ((1L << b).toLong - 1L).toInt
    //var test_j = j.toBinaryString
    //println("x len is : " + test.length + "moved x: " + (x>>>(n.toLong - (b + 1L))).toBinaryString)
    //j = (j.toLong & ((x&(~0L)) >>> (n - (b + 1)))).toInt
    //test_j = j.toBinaryString
    // to the same to get the bucket w
    //var w = ((1L << (n-(b+1))).toLong - 1L)
    //var test_w = w.toBinaryString
    //w = ((w.toLong) &x).toLong
    //test_w = w.toBinaryString
    //println("w len is : " + test_w.length)
    //var q = countLeadingZerosNum64(w  ,b+1)
    //if(q >= b) q = q - (b) // we move it to crop new 0 if the number isn't 32 bits
    val maximum = max(M(j),q+1)
    M.update(j,maximum)
  }
  // Do the same with bit moves
  //...
  // Use corrections from P. Flajolet
  def hloglogRaw_correction(estimate : Double, m : Int, M: Array[Int]): Double = {
    var Ex = 0.0
    // SMALL RANGE
    if(estimate <= 5*m/2){
      // number of zero registers
      var V = M.foldRight(0)((value, x) => value + (if(x==0) 1 else 0) )
      if(V!= 0) Ex = m*logBaseB(m/V,2)
      else Ex = estimate
    }
    val temp : Long = pow(2,32).toLong
    if(estimate <= 1/30*temp){
      Ex = estimate
    }
    if(estimate > 1/30*temp){
      Ex = -temp*logBaseB(1-(estimate/temp),2)
    }
    return estimate.toInt
  }
  // We will use file as a stream but it can be easily generalized to other types of streams
  // The filedata should be organised as (requirement from above :) )
  // format: timestamp,(renumbered) source host, (renumbered) destination host, source TCP port, destination
  // TCP port, number of data bytes (zero for ”pure-ack” packets)
  // we will apply it only to source hosts, destination hosts, and their pairs
  def hll_testOnFile(filename:String, b: Int, m : Int, alpha_m:Double, seed: Int, hashSize : Int, correction : Boolean) : Array[(Double,Double,Double)] ={
    var M_shosts= Array[Int]()
    var M_dhosts= Array[Int]()
    var M_bhosts = Array[Int]()
    if(correction) {
      M_shosts = Array.fill(m)(0)
      M_dhosts = Array.fill(m)(0)
      M_bhosts = Array.fill(m)(0)
    }
    else{
      M_shosts = Array.fill(m)(Integer.MIN_VALUE)
      M_dhosts = Array.fill(m)(Integer.MIN_VALUE)
      M_bhosts = Array.fill(m)(Integer.MIN_VALUE)
    }

    var test_s = scala.collection.mutable.Set[String]()
    var test_d = scala.collection.mutable.Set[String]()
    var test_b = scala.collection.mutable.Set[String]()
    // read from buffer as a stream
    val buffer = Source.fromFile(filename, "UTF-8")
    for(line <- buffer.getLines()){
      val split = line.split(" ")
      val shost = split(1)
      val dhost = split(2)
      val bhost = shost + dhost
      test_s.add(shost)
      test_d.add(dhost)
      test_b.add(bhost)
      hloglogRaw_step(shost,b,seed,hashSize,M_shosts) // source hosts
      hloglogRaw_step(dhost,b,seed,hashSize,M_dhosts) // destination hosts
      hloglogRaw_step(bhost,b,seed,hashSize,M_bhosts) // their pairs
    }
    println("Sources cardinality : " + test_s.size)
    println("Destination cardinality : " + test_d.size)
    println("Both cardinality : " + test_b.size)
    val indicator_s = (M_shosts.foldLeft(0.0)((x,y) => x + pow(0.5,y))).toDouble
    val indicator_d = (M_dhosts.foldLeft(0.0)((x,y) => x + pow(0.5,y))).toDouble
    val indicator_b = (M_bhosts.foldLeft(0.0)((x,y) => x + pow(0.5,y))).toDouble
    // Calculate raw
    var rawE_s = (alpha_m * pow(m.toDouble,2))/indicator_s
    var rawE_d = (alpha_m * pow(m.toDouble,2))/indicator_d
    var rawE_b = (alpha_m * pow(m.toDouble,2))/indicator_b
    // Put corrections for comparison if correction
    if(correction){
      rawE_s = hloglogRaw_correction(rawE_s,m,M_shosts)
      rawE_d = hloglogRaw_correction(rawE_d,m,M_dhosts)
      rawE_b = hloglogRaw_correction(rawE_b,m,M_bhosts)
    }




    return Array((rawE_s,rawE_d,rawE_b))
  }

  // ---------------------------------------------------------------------------
  // gives k shingles and updates universal set
  def kShingles(shingleSize : Int, doc : Array[String], universalSet : Set[String] ) : Set[String] ={
    var doc_shingles = Set[String]()
    for(i <- 0 until doc.length-shingleSize){
      var tempStr = ""
      for(j <- 0 until shingleSize){
        tempStr += doc(i+j)
      }
      //println(tempStr)
      doc_shingles += tempStr
    }
    universalSet ++= doc_shingles
    doc_shingles
  }
  // other shingle function for using minhash
 /* def kShingles4minhash(shingleSize : Int, doc : Array[String], universal : Set[String] ) : scala.collection.immutable.Set[Int] = {
    var doc_shingles = scala.collection.mutable.Set[String]()
    for(i <- 0 until doc.length-shingleSize){
      val universal_size = universal.size
      var tempStr = ""
      for(j <- 0 until shingleSize){
        tempStr += doc(i+j)
      }
      //println(tempStr)
      val index = universal.indexOf(tempStr)
      if(index == -1){
        universal.append(tempStr)
        doc_shingles += universal_size
      }
      else{
        doc_shingles += index
      }
    }
    doc_shingles.toSet
  }
  */
  // Exact similarity testing for k-shingles documents
  def jaccard(f1:String, f2:String, k:Int) : Double ={
    var universalSet = scala.collection.mutable.Set[String]()
    val doc1 = readFileToString(f1)
    val doc2 = readFileToString(f2)
    val doc1_s = kShingles(k, doc1,universalSet)
    val doc2_s = kShingles(k, doc2,universalSet)
    val both = doc1_s.intersect(doc2_s).size
    val non_zero = (doc1_s ++ doc2_s).size
    1.0-(both.toDouble/non_zero.toDouble)
  }
  // approximate similarity testing for k-shingles documents using minhash algorithm
  def jaccard_minhash(f1:String, f2:String, k:Int, hash_num:Int, seeds: Array[Int]) : Double ={
    var universalSet = scala.collection.mutable.Set[String]()
    val doc1 = readFileToString(f1)
    val doc2 = readFileToString(f2)
    val doc1_s = kShingles(k, doc1,universalSet)
    val doc2_s = kShingles(k, doc2,universalSet)

    val N = universalSet.size
    var estimators_1 = Array.fill(hash_num)(Integer.MAX_VALUE)
    var estimators_2 = Array.fill(hash_num)(Integer.MAX_VALUE)
    var i = 0
    universalSet foreach {
      x=>
      // first document
      if(doc1_s.contains(x)){
        for(j <- 0 until hash_num){
          val hash_j = MurmurHash3.arrayHash(i.toBinaryString.toCharArray,seeds(j))%N // modulo with number of elements
          if(hash_j < estimators_1(j)) estimators_1(j) = hash_j
        }
      }
      // second document
      if(doc2_s.contains(x)){
        for(j <- 0 until hash_num){
          val hash_j = MurmurHash3.arrayHash(i.toBinaryString.toCharArray,seeds(j))%N  // modulo with number of elements
          if(hash_j < estimators_2(j)) estimators_2(j) = hash_j
        }
      }
      i = i+1
    }
    var sum = 0
    for(i<-0 until hash_num){
      sum += convertBoolToInt(estimators_1(i)==estimators_2(i))
    }
    1.0-(sum.toDouble/hash_num)
  }
  // approximate similarity test for k-shingles with minihash quick
  def jaccard_minhash_quicker(f1: String, f2:String, k:Int, hash_num: Int, seeds: Array[Int]) : Double = {
    val hash_seed = 100
    // sets for tests
    var universalSet = scala.collection.mutable.Set[Int]()
    var set1 = scala.collection.mutable.Set[Int]()
    var set2 = scala.collection.mutable.Set[Int]()

    // documents as Strings
    val doc1 = readFileToString(f1)
    val doc2 = readFileToString(f2)
    // estimators
    var estimators_1 = Array.fill(hash_num)(Integer.MAX_VALUE)
    var estimators_2 = Array.fill(hash_num)(Integer.MAX_VALUE)

    // DOCUMENT 1
    for(i <- 0 until doc1.length-k){
      var tempStr = ""
      for(j <- 0 until k){
        tempStr += doc1(i+j)
      }
      val hashed = MurmurHash3.arrayHash(tempStr.toCharArray,hash_seed)
      if(!set1.contains(hashed)){
        set1 += hashed
        for(j <- 0 until hash_num){
          val hash_j = MurmurHash3.arrayHash(hashed.toBinaryString.toCharArray,seeds(j)) // modulo with number of elements
          if(hash_j < estimators_1(j)) estimators_1(j) = hash_j
        }
      }
    }
    // DOCUMENT 2
    for(i <- 0 until doc2.length-k){
      var tempStr = ""
      for(j <- 0 until k){
        tempStr += doc2(i+j)
      }
      val hashed = MurmurHash3.arrayHash(tempStr.toCharArray,hash_seed)
      if(!set2.contains(hashed)){
        set2 += hashed
        for(j <- 0 until hash_num){
          val hash_j = MurmurHash3.arrayHash(hashed.toBinaryString.toCharArray,seeds(j)) // modulo with number of elements
          if(hash_j < estimators_2(j)) estimators_2(j) = hash_j
        }
      }
    }
    // calculations
    var sum = 0
    for(i<-0 until hash_num){
      sum += convertBoolToInt(estimators_1(i)==estimators_2(i))
    }
    1.0-(sum.toDouble/hash_num)
  }
  def main(args : Array[String]) = {
    // TEST HLL
    /*
    val b =7
    val alpha = cal_alpha_m(b)
    val hll_test = hll_testOnFile(lbl_tcp_dir, b,pow(2,b).toInt,alpha,seed_hll,64,true)
    println(hll_test.mkString)
    */
    // TEST JACCARD
    val hashnum = 150
    val seeds = Array.fill(hashnum)(scala.util.Random.nextInt)
    val files = getListOfFiles(booksDir).map(_.toString)
    val f1 = files.head
    val second_num = 4//abs(scala.util.Random.nextInt)%files.size
    val f2 = files(second_num)
    val jaccard_distance_exact = jaccard(f1,f2,5)
    val jaccard_distance_est = jaccard_minhash(f1,f2,5,hash_num = hashnum,seeds = seeds)
    val jaccard_distance_est2 = jaccard_minhash_quicker(f1,f2,5,hashnum,seeds)
    println(jaccard_distance_exact, jaccard_distance_est,jaccard_distance_est2)
  }

}
