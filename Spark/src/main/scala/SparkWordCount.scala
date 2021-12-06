//make sure to have ran these commands and imported into hdfs /books/ for each
//wget -O eng.txt https://www.gutenberg.org/files/98/98-0.txt
//wget -O fr.txt https://www.gutenberg.org/files/32854/32854-8.txt
//wget -O ger.txt https://www.gutenberg.org/files/7205/7205-0.txt
//run with: spark-submit –-master=local[*] --class=SparkWordCount ./target/scala-2.10/simple-project_2.10-1.0.jar "books/eng.txt" "1000"
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

object SparkWordCount {
  def main(args: Array[String]) {
        // create Spark context with Spark configuration
        val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

        // args data
        val input1 = sc.textFile(args(0)) //"boooks/eng.txt"
        val threshold = args(1).toInt //"1000"

        // tokenize the books so that each word is a token
        val tokenizedData = input1.flatMap(_.split(" "))

        // map the tokenized text
        val mappedData = tokenizedData.map(word => (word, 1))

        // perform a MapReduce job that adds up the values associated with the words
        val reducedData = mappedData.reduceByKey((x,y) => x + y)

        // filter out words with fewer than threshold occurrences
        val filteredData = reducedData.filter(_._2 >= threshold)
        
        //run process
        val finalData = filteredData.collect()

        //create string with output data
        val output = finalData.mkString(", ")
        System.out.println(output)

        //run check for language determination
        val language = ""
        //english
        if(finalData.exists(_._1 == "the"))
        {
            language = "Text provided is likely english"
            System.out.println(language)
        }
        //french
        else if(finalData.exists(_._1 == "le")){
            language = "Text provided is likely french"
            System.out.println(language)
        }
        //german
        else if(finalData.exists(_._1 == "und")){
            language = "Text provided is likely german"
            System.out.println(language)
        }
        else{
            language = "Language unknown in text provided"
            System.out.println(language)
        }

        output = "Most frequently used words: " + output + "\n" + language

        Files.write(Paths.get("spark_output.txt"), output.getBytes(StandardCharsets.UTF_8))
 }
}