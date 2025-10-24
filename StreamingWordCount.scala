package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.Seq

object StreamingWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: HdfsWordCount <input_directory> <output_directory>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("checkpoint")

    // Create the FileInputDStream on the directory
    val lines = ssc.textFileStream(args(0))

    // Variables for sequence numbers
    var taskACounter = 1
    var taskBCounter = 1
    var taskCCounter = 1

    // Process words: remove non-letter words and filter short words
    val words = lines.flatMap(_.split(" "))
      .filter(word => word.matches("^[a-zA-Z]{3,}$"))

    /// Task A: Word frequency count
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val outputPath = s"${args(1)}/taskA-${"%03d".format(taskACounter)}"
        rdd.saveAsTextFile(outputPath)
        taskACounter += 1
      }
    }

    // Task B: Co-occurrence within lines
    val coOccurrences = lines.flatMap { line =>
      // Clean and filter words in the line
      val lineWords = line.split(" ")
        .filter(word => word.matches("^[a-zA-Z]{3,}$"))

      // Generate all possible pairs of words in the line
      for {
        i <- lineWords.indices
        j <- lineWords.indices
        if i != j
      } yield ((lineWords(i), lineWords(j)), 1)
    }.reduceByKey(_ + _)

    coOccurrences.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val outputPath = s"${args(1)}/taskB-${"%03d".format(taskBCounter)}"
        rdd.saveAsTextFile(outputPath)
        taskBCounter += 1
      }
    }

    // Task C: Continuous state updates for co-occurrences
    def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
      Some(newValues.sum + runningCount.getOrElse(0))
    }

    val continuousCoOccurrences = coOccurrences.updateStateByKey(updateFunction)

    continuousCoOccurrences.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val outputPath = s"${args(1)}/taskC-${"%03d".format(taskCCounter)}"
        rdd.saveAsTextFile(outputPath)
        taskCCounter += 1
      }
    }


    // Start the streaming context
    ssc.start()
    ssc.awaitTermination()
  }

}
