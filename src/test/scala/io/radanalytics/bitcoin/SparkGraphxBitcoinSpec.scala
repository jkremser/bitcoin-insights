/**
*
* This test "unit tests" the application with a local Spark master
*
*/
package io.radanalytics.bitcoin


import org.scalatest.{FlatSpec, BeforeAndAfterAll, GivenWhenThen, Matchers}


class SparkGraphxBitcoinSpec extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers  {


override def beforeAll(): Unit = {
    super.beforeAll()

 }

  
  override def afterAll(): Unit = {

    super.afterAll()
}



"Simple Test" should "be always successful" in {
	Given("Nothing")
	Then("success")
	// fetch results
	assert(42==42)
}


}
