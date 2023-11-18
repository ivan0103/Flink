import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import util.Protocol.{Commit, CommitGeo}

object helper_geoStream extends AssignerWithPeriodicWatermarks[CommitGeo] {
  override def getCurrentWatermark: Watermark = null // No new watermark available (also deprecated)

  override def extractTimestamp(t: CommitGeo, l: Long): Long = t.createdAt.getTime
}
