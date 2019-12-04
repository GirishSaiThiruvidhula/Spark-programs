class Account {
  val id = Account.newUniqueNumber()
  private var balance = 0.0;
  def deposit(amount: Double){ balance += amount}
  def balanceEnquiry() : Double = balance
}

object Account{
  private var lastNumber = 0
  private def newUniqueNumber() = {lastNumber +=1; lastNumber}
}


object CompaianObjects {
  def main(args: Array[String]): Unit = {
    val accountNo = new Account()
    println("Current new account number =" + accountNo.id)
    accountNo.deposit(1000)
    println("Current account balance =" + accountNo.balanceEnquiry())
    accountNo.deposit(5000)
    println("Current account balance =" + accountNo.balanceEnquiry())

    val accountNo2 = new Account()
    println("Current new account number =" + accountNo2.id)
    accountNo2.deposit(20000)
    println("Current account balance =" + accountNo2.balanceEnquiry())
    accountNo2.deposit(30000)
    println("Current account balance =" + accountNo2.balanceEnquiry())
  }
}
