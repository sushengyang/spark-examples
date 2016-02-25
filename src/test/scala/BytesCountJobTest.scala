import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import sql.BytesCountJob

class BytesCountJobTest extends FunSuite with SharedSparkContext {

  test("Sample unit test") {
      val input = List(
        "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"",
        "ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"",
        "ip1 - - [24/Apr/2011:04:14:36 -0400] \"GET /~strabal/grease/photo9/927-5.jpg HTTP/1.1\" 200 42011 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""
      )

      val expected =  "ip1,46322,138967"

      assert(BytesCountJob.transformInput(sc.parallelize(input)).collect()(0).mkString(",") === expected)
  }

}
