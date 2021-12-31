import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TestAdminView extends AnyFlatSpec with should.Matchers {
  "userMenu()" should "show the User Menu and options" in {
    AdminView.userMenu();
  }

  "manageQueries()" should "show the Query Management Menu and options" in {
    AdminView.manageQueries();
  }

  "manageUsers()" should "show the User Management Menu and options" in {
    AdminView.manageUsers();
  }
}