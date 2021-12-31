import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TestAdminController extends AnyFlatSpec with should.Matchers {
  "authorize(String, String)" should "bring the user into the ADMIN user's Home Screen, where they can see the database updates from the API" in {
    AdminController.authorize("admin", "admin");
  }

  it should "exit if the provided user credentials do not exist or are a BASIC user's credentials" in {
    AdminController.authorize("basic", "basic");
    AdminController.authorize("", "");
  }
}