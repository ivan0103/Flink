import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.{TimeCharacteristic, environment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows, WindowAssigner}
import util.Protocol.{Commit, CommitGeo, CommitSummary}
import util.{CommitGeoParser, CommitParser}
import java.util
import java.util.stream.Collectors

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.triggers.Trigger



/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignment {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
      * Setups the streaming environment including loading and parsing of the datasets.
      *
      * DO NOT TOUCH!
      */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
    //dummy_question(commitStream).print()
    //question_one(commitStream).print()
    //question_three(commitStream).print()
    //question_four(commitStream).print()
    //question_five(commitStream).print()
    //question_six(commitStream).print()
    //question_seven(commitStream).print()
    //question_eight(commitStream, commitGeoStream).print()
    question_nine(commitStream).print()

    /** Start the streaming environment. **/
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[String] = {
    input.map(_.sha)
  }

  /**
    * Write a Flink application which outputs the sha of commits with at least 20 additions.
    * Output format: sha
    */
  def question_one(input: DataStream[Commit]): DataStream[String] = {
    input
      .filter(_.stats.map(_.additions).get >= 20).map(_.sha)
  }

  /**
    * Write a Flink application which outputs the names of the files with more than 30 deletions.
    * Output format:  fileName
    */
  def question_two(input: DataStream[Commit]): DataStream[String] = {
    input.flatMap(a => a.files)
      .map(a => (a.filename match {
        case None => ""
        case Some(a) => a
      }, a.deletions))
      .filter(a => a._1 != "")
      .filter(a => a._2 > 30)
      .map(a => a._1)
  }

  /**
    * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
    * Output format: (fileExtension, #occurrences)
    */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input.flatMap(a => a.files)
      .map(a => (a.filename match {
        case None => ""
        case Some(a) => a
      }, 1))
      .map(a => (a._1.split("\\.").last, a._2))
      .filter(a => a._1 != "")
      .filter(a => a._1 == "scala" || a._1 == "java")
      .keyBy(a => a._1)
      .reduce((a, b) => (a._1, a._2 + b._2))
  }

  /**
    * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
    * Output format: (extension, status, count)
    */
  def question_four(input: DataStream[Commit]): DataStream[(String, String, Int)] = {
    input.flatMap(a => a.files)
      .map(a => (
        a.filename match {
          case None => ""
          case Some(a) => a.split("\\.").last
        },
        a.status match {
          case None => ""
          case Some(b) => b
        },
        a.changes,
        a.additions,
        a.deletions
      ))
      .filter(a => a._1 != "" && a._2 != "")
      .filter(a => a._1 == "py" || a._1 == "js")
      .keyBy(a => (a._1, a._2))
      .reduce((a, b) => (
        a._1,
        a._2,
        a._3 + b._3,
        a._4 + b._4,
        a._5 + b._5
      ))
      .map(a => (a._1, a._2, a._3))
  }

  /**
    * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
    * Make use of a non-keyed window.
    * Output format: (date, count)
    */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {

    val dateFormat = new SimpleDateFormat("dd-MM-yyyy")

    input.assignTimestampsAndWatermarks(helper_q6)
      .map(x => (x.commit.author.date, 1))
      .map(x => (dateFormat.format(x._1), x._2))
      .windowAll(SlidingEventTimeWindows.of(Time.days(1), Time.days(1)))
      //.keyBy(_._1)
      .reduce((a,b) =>(a._1, a._2 + b._2))
      .map(x =>(x._1, x._2))

  }

  /**
    * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
    * Compute every 12 hours the amount of small and large commits in the last 48 hours.
    * Output format: (type, count)
    */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = {

    //input.windowAll(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))



//    input.assignTimestampsAndWatermarks(helper_q6)
//      .filter(a => a.commit.committer.date.getTime > 0)
//      .map(a => a.files.map(b => b.changes).sum)
//      .map(a => (
//        if (a >= 0 && a <= 20) {
//          "small"
//        }else if (a > 20) {
//          "large"
//        }else {
//          ""
//        }
//      , 1))
//      .filter(a => a._1 != "")
//      .keyBy(a => a._1)
//      .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
//      .reduce((a, b) => (a._1, a._2 + b._2))

//    input.assignTimestampsAndWatermarks(helper_q6)
//      .map(a => a.stats match {
//        case None => -1
//        case Some(b) => b.total
//      })
//      .filter(a => a >= 0)
//      .map(a => (
//        if (a >= 0 && a <= 20) {
//          "small"
//        }else if (a > 20) {
//          "large"
//        }else {
//          ""
//        }
//      , 1))
//      .filter(a => a._1 != "")
//      .map(a => {
//        if (a._1 == "small") {
//          (1, 0)
//        } else {
//          (0, 1)
//        }
//      })
//      .windowAll(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
//      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
//      .map(a => List(("small", a._1), ("large", a._2)))
//      .flatMap(a => a)

//    input.assignTimestampsAndWatermarks(helper_q6)
//      .filter(a => a.commit.committer.date.getTime > 0)
//      .map(a => a.files.map(b => b.changes).sum)
//      .map(a => (
//        if (a >= 0 && a <= 20) {
//          "small"
//        }else if (a > 20) {
//          "large"
//        }else {
//          ""
//        }
//        , 1))
//      .filter(a => a._1 != "")
//      .map(a => {
//        if (a._1 == "small") {
//          (a._2, 0)
//        } else {
//          (0, a._2)
//        }
//      })
//      .windowAll(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
//      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
//      .map(a => List(("small", a._1), ("large", a._2)))
//      .flatMap(a => a)


    input.assignTimestampsAndWatermarks(helper_q6)
      .map(a => a.stats match {
        case None => -1
        case Some(b) => b.total
      })
      .filter(a => a >= 0)
      .map(a => (
        if (a >= 0 && a <= 20) {
          "small"
        }else if (a > 20) {
          "large"
        }else {
          ""
        }
        , 1))
      .filter(a => a._1 != "")
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
      .reduce((a, b) => (a._1, a._2 + b._2))


  }

  /**
    * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
    *
    * The fields of this case class:
    *
    * repo: name of the repo.
    * date: use the start of the window in format "dd-MM-yyyy".
    * amountOfCommits: the number of commits on that day for that repository.
    * amountOfCommitters: the amount of unique committers contributing to the repository.
    * totalChanges: the sum of total changes in all commits.
    * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
    *
    * Hint: Write your own ProcessWindowFunction.
    * Output format: CommitSummary
    */
  def question_seven(
      commitStream: DataStream[Commit]): DataStream[CommitSummary] = {

    val dateFormat = new SimpleDateFormat("dd-MM-yyyy")

    commitStream.assignTimestampsAndWatermarks(helper_q6)
      .map(a => (
        a.url.split("/")(4) + "/" + a.url.split("/")(5),
        dateFormat.format(a.commit.committer.date),
        1,
        List(a.commit.committer.name),
        a.stats match {
          case None => -1
          case Some(b) => b.total
        },
        ""
      ))
      .filter(a => a._5 >= 0)
      .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
      .reduce((a, b) => (
        a._1,
        a._2,
        a._3 + b._3,
        a._4 ++ b._4,
        a._5 + b._5,
        ""
      ))

      .map(a => (
        a._1,
        a._2,
        a._3,
        a._4.distinct.size,
        a._5,
        {
          val maxCommitter = a._4.groupBy(identity).mapValues(_.size).maxBy(_._2)

          val allMax = a._4.groupBy(identity).mapValues(_.size).filter(a => a._2 >= maxCommitter._2).toList

          if (allMax.size > 1) {
            allMax.map(a => a._1).distinct.sortBy(a => a)
          } else {
            List(maxCommitter._1)
          }
        }
      ))

      .filter(a => a._3 > 20 && a._4 <= 2)

      .map(a => new CommitSummary(
        a._1,
        a._2,
        a._3,
        a._4,
        a._5,
        a._6.mkString(",")
      ))


  }

  /**
    * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
    * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
    * Get the weekly amount of changes for the java files (.java extension) per continent. If no java files are changed in a week, no output should be shown that week.
    *
    * Hint: Find the correct join to use!
    * Output format: (continent, amount)
    */
  def question_eight(
      commitStream: DataStream[Commit],
      geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = {

    val timeAwareCommitStream = commitStream.assignTimestampsAndWatermarks(helper_q6)
    val timeAwareGeoStream = geoStream.assignTimestampsAndWatermarks(helper_geoStream)

     timeAwareCommitStream.join(timeAwareGeoStream)
      .where(a => a.sha)
      .equalTo(a => a.sha)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .apply((a, b) => (
        b.continent,
        a.files.filter(a => a.filename match {
          case None => false
          case Some(b) => b.endsWith(".java")
        }).map(a => a.changes).sum
      ))
      .keyBy(a => a._1)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .reduce((a, b) => (a._1, a._2 + b._2))
      .filter(a => a._2 > 0)




  }

  /**
    * Find all files that were added and removed within one day. Output as (repository, filename).
    *
    * Hint: Use the Complex Event Processing library (CEP).
    * Output format: (repository, filename)
    */
  def question_nine(
      inputStream: DataStream[Commit]): DataStream[(String, String)] = {

    val pattern = Pattern.begin[Commit]("start")

    inputStream.assignTimestampsAndWatermarks(helper_q6)
      .flatMap(a => a.files.map(b => (a.url.split("/")(4) + "/" + a.url.split("/")(5), a.commit.committer.date, b)))
      .map(a => (
        a._1,
        a._2,
        a._3.filename match {
          case None => ""
          case Some(b) => b
        },
        a._3.status match {
          case None => ""
          case Some(c) => c
        }
      ))
      .filter(a => a._3 != "" && a._4 != "")
      .map(a => (
        a._1,
        a._2,
        a._3,
        {
          if (a._4 == "added") {
            (1, 0)
          } else if (a._4 == "removed") {
            (0, 1)
          } else {
            (0, 0)
          }
        }
      ))
      .map(a => (
        a._1,
        a._2,
        a._3,
        a._4._1,
        a._4._2
      ))
      .keyBy(a => (a._1, a._2, a._3))
      .reduce((a, b) => (a._1, a._2, a._3, a._4 + b._4, a._5 + b._5))
      .map(a => (
        a._1,
        a._2,
        a._3,
        {
          if (a._4 > 0 && a._5 > 0) {
            // This file was added and removed on the same day
            true
          } else {
            false
          }
        }
      ))
      .filter(_._4)
      .map(a => (a._1, a._3))


  }

}