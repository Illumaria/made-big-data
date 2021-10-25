package models

import breeze.linalg.{DenseMatrix, DenseVector}

class LinearRegression {
  var weights: DenseVector[Double] = DenseVector.zeros[Double](size = 0)

  def fit(observations: DenseMatrix[Double], outputs: DenseVector[Double]): Unit = {
    /**
     * Given a set of data points as rows in a matrix and their corresponding outputs, produces a vector of weights
     * s.t. output(i) \approx observations(i) dot weights
     */
    val cov = observations.t * observations
    val scaled = observations.t * outputs
    weights = cov \ scaled
  }

  def predict(observations: DenseMatrix[Double]): DenseVector[Double] = {
    observations * weights
  }
}
