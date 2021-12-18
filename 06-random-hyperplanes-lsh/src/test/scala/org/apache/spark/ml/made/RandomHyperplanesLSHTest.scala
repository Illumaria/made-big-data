package org.apache.spark.ml.made

import com.google.common.io.Files
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class RandomHyperplanesLSHTest extends AnyFlatSpec with should.Matchers with WithSpark {
  val delta: Double = 0.0001
  lazy val data: DataFrame = RandomHyperplanesLSHTest._data
  lazy val vectors: Seq[Vector] = RandomHyperplanesLSHTest._vectors
  lazy val hyperplanes: Array[Vector] = RandomHyperplanesLSHTest._hyperplanes

  "Model" should "calculate hash" in {
    val RandomHyperplanesLSHModel: RandomHyperplanesLSHModel = new RandomHyperplanesLSHModel(randHyperPlanes = hyperplanes)
      .setInputCol("features")
      .setOutputCol("hashes")
    val testVector = Vectors.fromBreeze(breeze.linalg.Vector(3, 4, 5, 6))
    val sketch = RandomHyperplanesLSHModel.hashFunction(testVector)

    sketch.length should be (3)
    sketch(0)(0) should be (1.0)
    sketch(1)(0) should be (1.0)
    sketch(2)(0) should be (-1.0)
  }

  "Model" should "calculate hash distance" in {
    val RandomHyperplanesLSHModel: RandomHyperplanesLSHModel = new RandomHyperplanesLSHModel(randHyperPlanes = hyperplanes)
      .setInputCol("features")
      .setOutputCol("hashes")

    val testVector1 = Vectors.fromBreeze(breeze.linalg.Vector(3, 4, 5, 6))
    val sketch1 = RandomHyperplanesLSHModel.hashFunction(testVector1)

    val testVector2 = Vectors.fromBreeze(breeze.linalg.Vector(4, 3, 2, 1))
    val sketch2 = RandomHyperplanesLSHModel.hashFunction(testVector2)

    val similarity = RandomHyperplanesLSHModel.hashDistance(sketch1, sketch2)

    similarity should be ((1.0 / 3.0) +- delta)
  }

  "Model" should "calculate key distance" in {
    val RandomHyperplanesLSHModel: RandomHyperplanesLSHModel = new RandomHyperplanesLSHModel(randHyperPlanes = hyperplanes)
      .setInputCol("features")
      .setOutputCol("hashes")

    val testVector1 = Vectors.fromBreeze(breeze.linalg.Vector(3, 4, 5, 6))
    val testVector2 = Vectors.fromBreeze(breeze.linalg.Vector(4, 3, 2, 1))

    val keyDistance = RandomHyperplanesLSHModel.keyDistance(testVector1, testVector2)

    keyDistance should be ((1 - 0.7875) +- delta)
  }

  "Estimator" should "transform data" in {
    val estimator: RandomHyperplanesLSH = new RandomHyperplanesLSH()
      .setNumHashTables(2)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = estimator.fit(data)
    val transformedData = model.transform(data)

    transformedData.count() should be (3)
  }

  "Model" should "approx similarity join" in {
    val model: RandomHyperplanesLSHModel = new RandomHyperplanesLSHModel(randHyperPlanes = hyperplanes)
      .setInputCol("features")
      .setOutputCol("hashes")

    val approxData = model.approxSimilarityJoin(data, data, 1)

    approxData.count() should be (9)
  }

  "Estimator" should "work after re-read" in {
    val pipeline = new Pipeline().setStages(Array(
      new RandomHyperplanesLSH()
        .setNumHashTables(2)
        .setInputCol("features")
        .setOutputCol("hashes")
    ))

    val tempDir = Files.createTempDir()

    pipeline.write.overwrite().save(tempDir.getAbsolutePath)

    val model = Pipeline.load(tempDir.getAbsolutePath)
      .fit(data)
      .stages(0)
      .asInstanceOf[RandomHyperplanesLSHModel]

    val transformedData = model.transform(data)

    transformedData.count() should be (3)
  }

  "Model" should "work after re-read" in {
    val pipeline = new Pipeline().setStages(Array(
      new RandomHyperplanesLSHModel(randHyperPlanes = hyperplanes)
        .setInputCol("features")
        .setOutputCol("hashes")
    ))

    val model = pipeline.fit(data)

    val tempDir = Files.createTempDir()

    model.write.overwrite().save(tempDir.getAbsolutePath)

    val reRead: PipelineModel = PipelineModel.load(tempDir.getAbsolutePath)

    val transformedData = reRead.transform(data)

    transformedData.count() should be (3)
  }
}

object RandomHyperplanesLSHTest extends WithSpark {
  lazy val _vectors: Seq[Vector] = Seq(
    Vectors.dense(1, 2, 3, 4),
    Vectors.dense(5, 4, 9, 7),
    Vectors.dense(9, 6, 4, 5)
  )

  lazy val _hyperplanes: Array[Vector] = Array(
    Vectors.dense(1, -1, 1, 1),
    Vectors.dense(-1, 1, -1, 1),
    Vectors.dense(1, 1, -1, -1)
  )

  lazy val _data: DataFrame = {
    import sqlc.implicits._
    _vectors.map(x => Tuple1(x)).toDF("features")
  }
}