import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TestBasicView  extends AnyFlatSpec with should.Matchers {
  "mainMenu()" should "display the Main Menu options for BASIC users" in {
    BasicView.mainMenu();
  }
}