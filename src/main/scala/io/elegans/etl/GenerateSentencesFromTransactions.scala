package io.elegans.etl

import org.apache.hadoop.mapreduce.v2.proto.MRProtos.StringCounterGroupMapProto
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scopt.OptionParser
import org.apache.spark.rdd.RDD

/** Read a list of transactions, users and items and generate sentences */
object GenerateSentencesFromTransactions {

  lazy val textProcessingUtils = new TextProcessingUtils /* lazy initialization of TextProcessingUtils class */

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  /** Case class for command line variables with default values
    *
    * @param users the users data
    * @param items the items data
    * @param transactions the transactions data
    * @param output the path for output data
    */
  private case class Params(
    users: String = "users.csv",
    items: String = "items.csv",
    transactions: String = "transactions.csv",
	  output: String = "SENTENCES",
    format: String = "format1"
                           )

  /** Do all the spark initialization, the logic of the program is here
    *
    * @param params the command line parameters
    */
  private def doTokenization(params: Params) {
    val spark = SparkSession.builder().appName("GenerateSentencesFromTransactions").getOrCreate()

    val users_data = spark.read.format("csv").
      option("header", "true").
      option("delimiter", "\t").
      load(params.users)
    users_data.createOrReplaceTempView("users")

    val items_data = spark.read.format("csv").
      option("header", "true").
      option("delimiter", "\t").
      load(params.items)
    items_data.createOrReplaceTempView("items")

    val transactions_data = spark.read.format("csv").
      option("header", "true").
      option("delimiter", "\t").
      load(params.transactions)
    transactions_data.createOrReplaceTempView("transactions")

    val merged_data = spark.sql(
      "SELECT user, gender, age, job, item, rating, time, genres, title" +
        " FROM transactions t JOIN items i ON i.id = t.item JOIN users u ON u.id = t.user")

    val extracted_data_sentences: RDD[List[String]] =
      merged_data.rdd
        .map(x => {
          val title = if (x.isNullAt(8)) { "" } else x.getString(8)
          List(x.getString(0), x.getString(1),
            x.getString(2), x.getString(3),
            x.getString(4), x.getString(5),
            x.getString(6), x.getString(7), title
          )
        })

    val initialSet = List.empty[List[String]]
    val addToSet = (s: List[List[String]],
                    v: List[String]) => s ++ List(v)
    val mergePartitionSet = (s: List[List[String]],
                    v: List[List[String]]) => s ++ v

    val extracted_data = extracted_data_sentences
      .map(x => (x(0), x)).aggregateByKey(initialSet)(addToSet, mergePartitionSet).map(x => {
      (x._1, x._2.sortBy(z => z(6).toInt))
    })

    if(params.format == "format1") {
      val extractedSentences = extracted_data.flatMap(x => x._2)
        .map(x => { List("user_" + x(0), "gender_" + x(1),
          "age_" + x(2), "job_" + x(3), "itemid_" + x(4),
          "rating_" + x(5), x(7).split(",").mkString("_"), x(8).split(",").mkString(" ") ) })
        .map(x => x.mkString(" "))
      extractedSentences.saveAsTextFile(params.output + "/" + params.format) /* write the output in plain text format */
    } else if(params.format == "format2") {
      val extractedUserSentences = extracted_data.flatMap(x => x._2).map(x => { List("user_" + x(0), "gender_" + x(1),
          "age_" + x(2), "job_" + x(3), "itemid_" + x(4), "rating_" + x(5)) } )
        .map(x => x.mkString(" "))
      extractedUserSentences.saveAsTextFile(params.output + "/" + params.format + "users") /* write the output in plain text format */

      val extractedItemSentences = extracted_data.flatMap(x => x._2).map(x => {
        List("itemid_" + x(4), x(7).split(",").mkString(" "), x(8).split(",").mkString(" ")) } )
        .map(x => x.mkString(" "))
      extractedItemSentences.saveAsTextFile(params.output + "/" + params.format + "items") /* write the output in plain text format */
    } else if (params.format == "format3") {
      val extractedSentences = extracted_data.flatMap(x => x._2)
        .map(x => { List("user_" + x(0), "gender_" + x(1),
          "age_" + x(2), "job_" + x(3), "itemid_" + x(4),
          "rating_" + x(5), x(7).split(",").mkString(" "), x(8).split(",").mkString(" ") ) })
        .map(x => x.mkString(" "))
      extractedSentences.saveAsTextFile(params.output + "/" +   params.format) /* write the output in plain text format */
    } else if (params.format == "format4") {
      val user_feat_selection = List(1, 2, 3)
      val item_action_gen_feat_selection = List(4, 6)
      val extractedPairs: RDD[(String, String)] = extracted_data.map(user_actions => {
        val features = user_actions._2.map(x => {
          List("user_" + x(0), "gender_" + x(1),
            "age_" + x(2), "job_" + x(3), "itemid_" + x(4) + "_" + "rating_" + x(5), x(6)/* time */, x(7) /*genres*/)
        }).map(x => {
          val user_features_list =  user_feat_selection.map(x)
          val item_action_gen_features_list_1 = item_action_gen_feat_selection.map(x)
          val item_action_gen_features_list =
            (item_action_gen_features_list_1.dropRight(1)
              ++ item_action_gen_features_list_1.last.split(",").toList.map(y => "genres_" + y))

          val feat_combination = user_features_list cross item_action_gen_features_list
          feat_combination
        })

        val item_action_features_list = user_actions._2
          .map(x => { ("itemid_" + x(4) + "_" + "rating_" + x(5), x(6).toInt) }).sortBy(_._2).map(x => x._1)

        val item_action_pairs = item_action_features_list.sliding(2).filter(_.length > 1).map(y => (y(0), y(1)))

        features.flatten ++ item_action_pairs.toList
      }).flatMap(x => x)
      extractedPairs.map(x => x._1 + " " + x._2).saveAsTextFile(params.output + "/" +   params.format) /* write the output in plain text format */
    } else if (params.format == "format5") {
      val extractedPairs: RDD[(String, String)] = extracted_data.map(user_actions => {
        val item_action_features_list = user_actions._2
          .map(x => "itemid_" + x(4))
        //val item_action_pairs = item_action_features_list.sliding(2).filter(_.length > 1).map(y => (y(0), y(1)))
        val item_action_pairs = item_action_features_list cross item_action_features_list
        item_action_pairs.filter(x => x._1 != x._2)
      }).flatMap(x => x)
      extractedPairs.map(x => x._1 + " " + x._2).saveAsTextFile(params.output + "/" +   params.format) /* write the output in plain text format */
    } else {
      spark.stop()
      println("Error: format not supported : " + params.format)
      System.exit(100)
    }

    spark.stop()
  }

  /** The main function, this function must be given in order to run a stand alone program
    *
    * @param args will contain the command line arguments
    */
  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("TokenizeSentences") { /* initialization of the parser */
      head("create sentences from user iteractions list of sentences with spark")
      help("help").text("prints this usage text")
      opt[String]("users")
        .text(s"the users data file" +
          s"  default: ${defaultParams.users}")
        .action((x, c) => c.copy(users = x))
      opt[String]("items")
        .text(s"the items data file" +
          s"  default: ${defaultParams.items}")
        .action((x, c) => c.copy(items = x))
      opt[String]("transactions")
        .text(s"the transactions file" +
          s"  default: ${defaultParams.transactions}")
        .action((x, c) => c.copy(transactions = x))
      opt[String]("output")
        .text(s"the destination directory for the output" +
          s"  default: ${defaultParams.output}")
        .action((x, c) => c.copy(output = x))
      opt[String]("format")
        .text(s"the format of the the output" +
          s"  default: ${defaultParams.format}")
        .action((x, c) => c.copy(format = x))

    }

    parser.parse(args, defaultParams) match { /* parsing of the command line options */
      case Some(params) =>
        doTokenization(params)
      case _ =>
        sys.exit(1)
    }
  }
}
