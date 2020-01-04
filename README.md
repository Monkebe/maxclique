# maxclique
Spark Library For Finding Maximal Cliques In a Graph. For every vertex in a graph (that is in a clique of size >=3), it returns a list of lists representing all of the maximally-sized cliques that the vertex is a part of.

It works by finding every clique in an inductive method by building cliques of size N by appropriately merging cliques of size N/2.

To run, just supply a dataset of Edges with vertex identifiers:
```
    val spark: SparkSession = # Input
    import spark.implicits._
    val edgeDS: Dataset[(Long, Long)] = # INPUT
    val vertexMaximalCliquesDS: Dataset[VertexMaximalCliques] = MaxCliqueRunner.run(edgeDS, spark)
```
