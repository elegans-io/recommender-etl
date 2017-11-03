import sbtsparksubmit.SparkSubmitPlugin.autoImport._

object SparkSubmit {
  lazy val settings = SparkSubmitSetting(
    SparkSubmitSetting("etl",
      Seq("--class", "io.elegans.etl.GenerateSentencesFromTransactions"))
  )
}
