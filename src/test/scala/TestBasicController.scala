import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TestBasicController  extends AnyFlatSpec with should.Matchers {
  "authorize(String, String)" should "bring the user into the BASIC user's Home Screen, where they can see the database updates from the API" in {
    if (!HiveDBManager.usernameExists("basic"))
      HiveDBManager.addUser("basic", "basic");
    BasicController.authorize("basic", "basic");
  }

  it should "exit if the provided user credentials do not exist or are an Admin's credentials" in {
    BasicController.authorize("admin", "admin");
    BasicController.authorize("", "");
  }
}