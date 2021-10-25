import breeze.linalg.{DenseMatrix, DenseVector, csvread, csvwrite}
import models.LinearRegression
import utils.{crossValidation, getLogger}

import java.io.File


object Main {
  /**
   * Dataset link: https://www.kaggle.com/mohansacharya/graduate-admissions
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Provide paths for train data, test data, and output predictions.")
      return -1
    }

    // Create logger:
    val logger = getLogger(name = "Logistic Regression", outputPath = "app.log")

    val trainPath: File = new File(args(0))
    val testPath: File = new File(args(1))
    val outputPath: File = new File(args(2))

    logger.info(s"Train data path: $trainPath")
    logger.info(s"Test data path: $testPath")
    logger.info(s"Output path: $outputPath")

    // Load train data:
    val train: DenseMatrix[Double] = csvread(trainPath, separator = ',', skipLines = 1)

    // Create model:
    val model = new LinearRegression()

    // Perform k-fold cross-validation:
    crossValidation(model, train, numFolds = 5, logger = logger)

    // Train on full dataset:
    val xTrain: DenseMatrix[Double] = train(::, 0 to -2)
    val yTrain: DenseVector[Double] = train(::, -1)
    model.fit(xTrain, yTrain)

    // Load test data and get predictions:
    val test: DenseMatrix[Double] = csvread(testPath, separator = ',', skipLines = 1)
    val testPredictions: DenseVector[Double] = model.predict(test)

    // Save predictions to file.
    // Cast to DenseMatrix is needed because csvwrite doesn't work with DenseVectors.
    // Transpose is performed for better view.
    csvwrite(outputPath, testPredictions.asDenseMatrix.t)
    // println(s"Predictions saved to $outputPath")
    logger.info(s"Predictions saved to $outputPath")
  }
}
