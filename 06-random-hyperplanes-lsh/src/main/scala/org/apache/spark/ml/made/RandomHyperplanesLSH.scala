package org.apache.spark.ml.made

import org.apache.spark.ml.feature.{LSH, LSHModel}
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasSeed
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.util.Random

class RandomHyperplanesLSH(override val uid: String) extends LSH[RandomHyperplanesLSHModel] with HasSeed {

  def this() = this(Identifiable.randomUID("RandomHyperplanesLSH"))

  override def setInputCol(value: String): this.type = super.setInputCol(value)

  override def setOutputCol(value: String): this.type = super.setOutputCol(value)

  override def setNumHashTables(value: Int): this.type = super.setNumHashTables(value)

  def setSeed(value: Long): this.type = set(seed, value)

  override protected[this] def createRawLSHModel(inputDim: Int): RandomHyperplanesLSHModel = {
    val rand = new Random($(seed))
    val randHyperPlanes: Array[Vector] = {
      Array.fill($(numHashTables)) {
        val randArray: Array[Double] = Array.fill(inputDim)(rand.nextInt(3) - 1.0)
        Vectors.fromBreeze(breeze.linalg.Vector(randArray))
      }
    }
    new RandomHyperplanesLSHModel(uid, randHyperPlanes)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    validateAndTransformSchema(schema)
  }
}

class RandomHyperplanesLSHModel private[made](override val uid: String, private[made] val randHyperPlanes: Array[Vector])
  extends LSHModel[RandomHyperplanesLSHModel] {

  private[made] def this(randHyperPlanes: Array[Vector]) =
    this(Identifiable.randomUID("RandomHyperplanesLSH"), randHyperPlanes)

  override def setInputCol(value: String): this.type = super.set(inputCol, value)

  override def setOutputCol(value: String): this.type = super.set(outputCol, value)

  override protected[ml] def hashFunction(elems: Vector): Array[Vector] = {
    val hashValues = randHyperPlanes.map(
      randHyperPlane => Math.signum(elems.dot(randHyperPlane))
    )
    hashValues.map(Vectors.dense(_))
  }

  override protected[ml] def keyDistance(x: Vector, y: Vector): Double = {
    if (Vectors.norm(x, 2) == 0 || Vectors.norm(y, 2) == 0) {
      1.0
    } else {
      1.0 - x.dot(y) / (Vectors.norm(x, 2) * Vectors.norm(y, 2))
    }
  }

  override protected[ml] def hashDistance(x: Seq[Vector], y: Seq[Vector]): Double = {
    x.zip(y).map(item => if (item._1 == item._2) 1.0 else 0.0).sum / x.size
  }

  override def copy(extra: ParamMap): RandomHyperplanesLSHModel = {
    val copied = new RandomHyperplanesLSHModel(uid, randHyperPlanes).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new RandomHyperplanesLSHModel.RandomHyperplanesLSHModelWriter(this)
}

object RandomHyperplanesLSH extends DefaultParamsReadable[RandomHyperplanesLSH] {
  override def load(path: String): RandomHyperplanesLSH = super.load(path)
}

object RandomHyperplanesLSHModel extends MLReadable[RandomHyperplanesLSHModel] {
  override def read: MLReader[RandomHyperplanesLSHModel] = new RandomHyperplanesLSHModelReader

  override def load(path: String): RandomHyperplanesLSHModel = super.load(path)

  private[RandomHyperplanesLSHModel] class RandomHyperplanesLSHModelWriter(instance: RandomHyperplanesLSHModel)
    extends MLWriter {

    private case class Data(randHyperPlanes: Matrix)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)

      val numRows = instance.randHyperPlanes.length
      require(numRows > 0)
      val numCols = instance.randHyperPlanes.head.size

      val values = instance.randHyperPlanes.map(_.toArray).reduce(Array.concat(_, _))
      val randMatrix = Matrices.dense(numRows, numCols, values)
      val data = Data(randMatrix)

      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(path + "/data")
    }
  }

  private class RandomHyperplanesLSHModelReader extends MLReader[RandomHyperplanesLSHModel] {
    /** Checked against metadata when loading model */
    private val className = classOf[RandomHyperplanesLSHModel].getName

    override def load(path: String): RandomHyperplanesLSHModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val data = sparkSession.read.parquet(path + "/data")
      val Row(randHyperPlanes: Matrix) = MLUtils.convertMatrixColumnsToML(data, "randHyperPlanes")
        .select("randHyperPlanes")
        .head()
      val model = new RandomHyperplanesLSHModel(metadata.uid, randHyperPlanes.rowIter.toArray)

      metadata.getAndSetParams(model)
      model
    }
  }
}