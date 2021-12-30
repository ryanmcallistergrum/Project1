import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TestController  extends AnyFlatSpec with should.Matchers {
  "intro()" should "start the Introductory menu where the user can register an account, log in, or exit" in {
    Controller.intro();
  }
}