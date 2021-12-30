import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TestMain extends AnyFlatSpec with should.Matchers {
  "main(Array[String])" should "start the application" in {
    Main.main(Array[String]());
  }
}
