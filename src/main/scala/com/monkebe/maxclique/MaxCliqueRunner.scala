package com.monkebe.maxclique


import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object MaxCliqueRunner {


  /**
    * Converts Dataset[SubClique] to Dataset[SubCliqueCriteria], by generating all possible criteria for every SubClique.
    * It does this by generating all criteriaSize-combination of neighbors for each SubClique and making a SubCliqueCriteria for each.
    * @param ds: Dataset of SubClique
    * @param criteriaSize: Size of criteria to generate
    * @param ss: SparkSession
    * @return - Dataset[SubCliqueCriteria] of all possible SubCliqueCriteria generated from the input SubCliques
    */
  private[maxclique] def subCliqueToSubCliqueCriteria(ds: Dataset[SubClique], criteriaSize: Int, ss: SparkSession): Dataset[SubCliqueCriteria] = {
    import ss.implicits._

    ds.flatMap(subClique =>
      subClique
      .candidates
      .combinations(criteriaSize)
      .toList
      .map(criteria => SubCliqueCriteria(subClique.members, subClique.candidates, criteria.sorted)))
  }

  /**
    * Takes in a List of Dataset[SubClique], merges them, and then finds the SubCliques
    * of maximal size for each vertex id.
    * @param subCliqueList - List of Dataset[SubClique] for different sizes
    * @param ss - SparkSession
    * @return - Dataset[VertexMaximalCliques] of pairs of (vertexIds, List of maximal-size cliques)
    */
  private[maxclique] def reduceSubCliqueDatasetList(subCliqueList: List[Dataset[SubClique]], ss: SparkSession): Dataset[VertexMaximalCliques] = {
    import ss.implicits._
    subCliqueList.reduce((dsA, dsB) => dsA.union(dsB))
      .flatMap(subClique => subClique.members.map(member => (member, subClique.members)))
      .groupByKey(pair => pair._1)
      .mapValues(pair => (pair._2.length, Set(pair._2)))
      .reduceGroups((sizeSetA, sizeSetB) => {
        if(sizeSetA._1 > sizeSetB._1) {
          sizeSetA
        } else if (sizeSetA._1 < sizeSetB._1) {
          sizeSetB
        } else {
          (sizeSetA._1, sizeSetA._2.union(sizeSetB._2))
        }
      })
      .map(pair => VertexMaximalCliques(pair._1, pair._2._2.toList.sortWith(
        (memA, memB) => {
          memA.zip(memB)
            .map(pair => pair._1.compareTo(pair._2))
            .filterNot(num => num == 0)
            .collectFirst({ case num: Int => num == -1 })
            .getOrElse(false)
        })))
  }

  /**
    * Given an input of edges, will output all of the maximal cliques that each vertex is a part of.
    * Assumes that input is bidirectional and undirected if (x,y) is in edgeDS, (y,x) must also be in edgeDS.
    * @param edgeDS - Dataset[(Long, Long)] representing (src,dst) of vertex identifiers.
    * @param ss - SparkSession
    * @return - Dataset of VertexMaximalClique objects: (vertex, list of cliques of maximal size each clique being represented as a list)
    */
  def run(edgeDS: Dataset[(Long, Long)], ss: SparkSession): Dataset[VertexMaximalCliques] = {
    import ss.implicits._

    val subCliqueOne: Dataset[SubClique] = edgeDS.groupByKey(pair => pair._1)
      .mapValues(pair => pair._2)
      .mapGroups((a,b) => SubClique(List(a),b.toList.sorted))

    val subCliqueListBuffer: ListBuffer[Dataset[SubClique]] = ListBuffer(subCliqueOne)

    breakable {
      while (!subCliqueListBuffer.last.isEmpty) {
        val subCliqueSize: Int = subCliqueListBuffer.size + 1
        var leftSubCliqueSize: Int = -1
        var rightSubCliqueSize: Int = -1
        var leftSubClique: Dataset[SubClique] = null
        var rightSubClique: Dataset[SubClique] = null

        if (subCliqueSize % 2 == 0) {
          leftSubCliqueSize = subCliqueSize / 2
          rightSubCliqueSize = subCliqueSize / 2
          leftSubClique = subCliqueListBuffer(leftSubCliqueSize - 1)
          rightSubClique = subCliqueListBuffer(rightSubCliqueSize - 1)
        } else {
          leftSubCliqueSize = subCliqueSize / 2
          rightSubCliqueSize = subCliqueSize / 2 + 1
          leftSubClique = subCliqueListBuffer(leftSubCliqueSize - 1)
          rightSubClique = subCliqueListBuffer(rightSubCliqueSize - 1)
        }

        val leftSubCliqueCriteria: Dataset[SubCliqueCriteria] = subCliqueToSubCliqueCriteria(leftSubClique, rightSubCliqueSize, ss)

        val rightSubCliqueCriteria: Dataset[SubCliqueCriteria] = subCliqueToSubCliqueCriteria(rightSubClique, leftSubCliqueSize, ss)

        // Merge subcliques into 1 larger subclique
        val nextSubClique: Dataset[SubClique] = leftSubCliqueCriteria
          .joinWith(rightSubCliqueCriteria,
            leftSubCliqueCriteria("members")===rightSubCliqueCriteria("criteria")
              && leftSubCliqueCriteria("criteria")===rightSubCliqueCriteria("members"))
          .map(pair => JoinedSubCliqueCriteria(pair._1, pair._2))
          .map(joinedSubCliqueCriteria => {
            val members: List[Long] = joinedSubCliqueCriteria.leftCriteria.members.union(joinedSubCliqueCriteria.rightCriteria.members).sorted
            val candidates: List[Long] = (joinedSubCliqueCriteria.leftCriteria.candidates.intersect(joinedSubCliqueCriteria.rightCriteria.candidates).toSet -- members.toSet).toList.sorted
            SubClique(members, candidates)
          })

        if (nextSubClique.count() == 0)
          break()
        subCliqueListBuffer += nextSubClique
      }
    }

    // Now that we have a list of subcliques by size, we can start removing some by finding maximals for each
    reduceSubCliqueDatasetList(subCliqueListBuffer.toList.slice(1, subCliqueListBuffer.size), ss)
  }
}
