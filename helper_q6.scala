import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import util.Protocol.Commit

object helper_q6 extends AssignerWithPeriodicWatermarks[Commit] {
  override def getCurrentWatermark: Watermark = null // No new watermark available (also deprecated)

  override def extractTimestamp(t: Commit, l: Long): Long = t.commit.committer.date.getTime
}
