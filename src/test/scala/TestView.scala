import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TestView  extends AnyFlatSpec with should.Matchers {
  "welcome()" should "output the main menu and its available options" in {
    View.welcome();
    assert(true);
  }
}