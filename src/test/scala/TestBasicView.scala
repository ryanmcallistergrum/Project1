import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TestBasicView  extends AnyFlatSpec with should.Matchers {
  "userMenu()" should "display the User Menu options for BASIC users" in {
    BasicView.userMenu();
  }
}