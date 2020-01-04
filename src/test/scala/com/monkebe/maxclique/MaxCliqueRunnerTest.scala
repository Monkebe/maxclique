package com.monkebe.maxclique

import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class MaxCliqueRunnerTest extends FunSuite{


  test("test subCliqueToSubCliqueCriteria 1") {
    val criteriaSize: Int = 2
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val subCliqueDS: Dataset[SubClique] = spark.sqlContext.read.json("src/test/resources/SubCliques1.txt").as[SubClique]

    val subCliqueCriteriaDS: Dataset[SubCliqueCriteria] = MaxCliqueRunner.subCliqueToSubCliqueCriteria(subCliqueDS, criteriaSize, spark)

    subCliqueCriteriaDS.collect().toList.map(subCliqueCriteria => subCliqueCriteria.criteria).equals(
      List(
        List(4,5),
        List(4,6),
        List(5,6)
      )
    )
  }

  test("test subCliqueToSubCliqueCriteria 2") {
    val criteriaSize: Int = 1
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val subCliqueDS: Dataset[SubClique] = spark.sqlContext.read.json("src/test/resources/SubCliques1.txt").as[SubClique]

    val subCliqueCriteriaDS: Dataset[SubCliqueCriteria] = MaxCliqueRunner.subCliqueToSubCliqueCriteria(subCliqueDS, criteriaSize, spark)

    subCliqueCriteriaDS.collect().toList.equals(
      List(
        SubCliqueCriteria(List(1,2,3), List(7,8), List(7)),
        SubCliqueCriteria(List(1,2,3), List(7,8), List(8)),
        SubCliqueCriteria(List(4,5,6), List(7,8), List(7)),
        SubCliqueCriteria(List(4,5,6), List(7,8), List(8))
      )
    )
  }

  test("test reduceSubCliqueDatasetList 1") {
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val subCliquesSizeOne: Dataset[SubClique] = List(SubClique(List(1), List(2,3,4,5,6))).toDS()
    val subCliquesSizeTwo: Dataset[SubClique] = List(SubClique(List(1,3), List(2,4,5,6))).toDS()
    val subCliquesSizeThree: Dataset[SubClique] = List(SubClique(List(1,2,5), List(3,4,6))).toDS()

    val subCliqueList: List[Dataset[SubClique]] = List(subCliquesSizeOne, subCliquesSizeTwo, subCliquesSizeThree)

    val vertexMaximalCliques: Dataset[VertexMaximalCliques] = MaxCliqueRunner.reduceSubCliqueDatasetList(subCliqueList, spark)

    assert(vertexMaximalCliques.collect().toList.sortBy(vertexMaximalCliques => vertexMaximalCliques.vertexId).equals(
     List(
       VertexMaximalCliques(1, List(List(1, 2, 5))),
       VertexMaximalCliques(2, List(List(1, 2, 5))),
       VertexMaximalCliques(3, List(List(1, 3))),
       VertexMaximalCliques(5, List(List(1, 2, 5))),
     )))
  }

  test("test reduceSubCliqueDatasetList 2") {
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val subCliquesSizeThree: Dataset[SubClique] = List(
      SubClique(List(1,2,3), List(4)),
      SubClique(List(1,2,4), List(3))
    ).toDS()

    val subCliqueList: List[Dataset[SubClique]] = List(subCliquesSizeThree)

    val vertexMaximalCliques: Dataset[VertexMaximalCliques] = MaxCliqueRunner.reduceSubCliqueDatasetList(subCliqueList, spark)

    assert(vertexMaximalCliques.collect().toList.sortBy(vertexMaximalCliques => vertexMaximalCliques.vertexId).equals(
      List(
        VertexMaximalCliques(1, List(List(1, 2, 3), List(1, 2, 4))),
        VertexMaximalCliques(2, List(List(1, 2, 3), List(1, 2, 4))),
        VertexMaximalCliques(3, List(List(1, 2, 3))),
        VertexMaximalCliques(4, List(List(1, 2, 4)))
      )))
  }

  test("test run 1") {
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._


    val edgeDS: Dataset[(Long, Long)] = spark.sqlContext.read.json("src/test/resources/edges1.txt").as[(Long, Long)]

    val vertexMaximalCliquesDS: Dataset[VertexMaximalCliques] = MaxCliqueRunner.run(edgeDS, spark)

    vertexMaximalCliquesDS.collect().toList.sortBy(vmc => vmc.vertexId).equals(
      List(
        VertexMaximalCliques(1, List(List(1,2,3))),
        VertexMaximalCliques(2, List(List(1,2,3))),
        VertexMaximalCliques(3, List(List(1,2,3), List(3,4,5))),
        VertexMaximalCliques(4, List(List(3,4,5))),
        VertexMaximalCliques(5, List(List(3,4,5)))
      )
    )
  }

  test("test run 2") {
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._


    val edgeDS: Dataset[(Long, Long)] = spark.sqlContext.read.json("src/test/resources/edges2.txt").as[(Long, Long)]

    val vertexMaximalCliquesDS: Dataset[VertexMaximalCliques] = MaxCliqueRunner.run(edgeDS, spark)

    vertexMaximalCliquesDS.collect().toList.sortBy(vmc => vmc.vertexId).equals(
      List(
        VertexMaximalCliques(1, List(List(1,2,3))),
        VertexMaximalCliques(2, List(List(1,2,3))),
        VertexMaximalCliques(3, List(List(1,2,3))),
        VertexMaximalCliques(4, List())
      )
    )
  }
}
