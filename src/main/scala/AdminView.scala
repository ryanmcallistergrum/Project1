object AdminView {
  def userMenu() : Unit = {
    println("Welcome to the User Menu. Please select from one of the following options:");
    println("1. Return to Home Screen");
    println("2. Manage Queries");
    println("3. Manage Users");
    println("4. Change Username");
    println("5. Change Password");
    println("6. Log Out");
  }

  def manageQueries() : Unit = {
    println("Welcome to the Query Management Menu. Please select from one of the following options:");
    println("1. Execute Query");
    println("2. Rename Query");
    println("3. Write Query");
    println("4. Delete Query");
  }

  def manageUsers() : Unit = {
    println("Welcome to the User Management Menu. Please select from one of the following options:");
    println("1. Delete Basic User");
    println("2. Promote Basic User to Admin");
  }
}