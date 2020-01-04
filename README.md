# maxclique
Spark Library For Finding Maximal Cliques In a Graph

To run, just supply a dataset of Edges with vertex identifiers:
```
    val spark: SparkSession = # Input
    import spark.implicits._
    val edgeDS: Dataset[(Long, Long)] = # INPUT
    val vertexMaximalCliquesDS: Dataset[VertexMaximalCliques] = MaxCliqueRunner.run(edgeDS, spark)
```
