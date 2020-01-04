package com.monkebe

import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.types._

package object maxclique {

  case class SubClique(members: List[Long], candidates: List[Long])

  case class SubCliqueCriteria(members: List[Long], candidates: List[Long], criteria: List[Long])

  case class JoinedSubCliqueCriteria(leftCriteria: SubCliqueCriteria, rightCriteria: SubCliqueCriteria)

  case class VertexMaximalCliques(vertexId: Long, maximalCliques: List[List[Long]])
}
