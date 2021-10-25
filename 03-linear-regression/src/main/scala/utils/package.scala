import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.numerics.abs
import breeze.stats.mean
import models.LinearRegression

import java.util.logging.{FileHandler, Logger, SimpleFormatter}

package object utils {
  def getLogger(name: String, outputPath: String): Logger = {
    System.setProperty(
      "java.util.logging.SimpleFormatter.format",
      "%1$tF %1$tT %4$s %5$s%6$s%n"
    )

    val logger = Logger.getLogger(name)
    val handler = new FileHandler(outputPath)
    val formatter = new SimpleFormatter()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger
  }

  def meanAbsoluteError(yTrue: DenseVector[Double], yPred: DenseVector[Double]): Double = {
    mean(abs(yTrue - yPred))
  }

  def crossValidation(model: LinearRegression, data: DenseMatrix[Double], numFolds: Int, logger: Logger): Unit = {
    val step: Int = data.rows / numFolds

    for (i <- 0 until numFolds) {
      val trainIndices: IndexedSeq[Int] = IndexedSeq.range(0, i * step) ++ IndexedSeq.range((i + 1) * step, data.rows)
      val validIndices: IndexedSeq[Int] = IndexedSeq.range(i * step, (i + 1) * step)

      val trainFold: DenseMatrix[Double] = data(trainIndices, ::).toDenseMatrix
      val xTrain: DenseMatrix[Double] = trainFold(::, 0 to -2)
      val yTrain: DenseVector[Double] = trainFold(::, -1)

      val validFold: DenseMatrix[Double] = data(validIndices, ::).toDenseMatrix
      val xValid: DenseMatrix[Double] = validFold(::, 0 to -2)
      val yValid: DenseVector[Double] = validFold(::, -1)

      model.fit(xTrain, yTrain)

      val maeScore: Double = meanAbsoluteError(yValid, model.predict(xValid))
      logger.info(s"Fold $i, MAE-score: $maeScore")
    }
  }
}
