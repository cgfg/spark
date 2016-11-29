# PageRank
bin/spark-shell --driver-memory 1g
sc.setLogLevel("INFO")
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
val graph = GraphLoader.edgeListFile(sc, "./web-Google.txt", false, 400)
val ranks = graph.pageRank(0.01).vertices

10g 35s
5g 36s
3g 42s
2g 37s
1.5g  54s-59s LFU
1.5g  57s-61s LRU
1.5g  58s-61s FIFO