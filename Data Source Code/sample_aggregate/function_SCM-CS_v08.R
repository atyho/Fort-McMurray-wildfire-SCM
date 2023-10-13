###############################################################################
# Article: Synthetic Control Method: Inference, Sensitivity Analysis and
#          Confidence Sets
# Authors: Sergio Firpo and Vitor Possebom
# Code by: Vitor Possebom
# Function: Implement Confidence Sets and the Sensitivity Analysis Mechanism
#           for the Synthetic Control Estimator.
# Version: 08 - This function to implements the confidences Sets and the
#          Sensitivity Analysis Mechanism for the Synthetic Control Estimator
#          proposed by Firpo and Possebom (2018). We use the RMSPE test
#          statistic to compute confidence sets in this code. The user may want
#          to manually change the test statistic. Any questions, suggestions,
#          comments, critics can be sent to Vitor Possebom
#          (vitoraugusto.possebom@yale.edu). In particular, the user may want
#          to manually change the graphical parameters at the end of this code.
###############################################################################
SCM.CS <- function(Ymat, weightsmat, treated, T0, phi, v, precision, type,
                   significance, plot) {
  # 'Ymat' is a matrix that contains the realized outcome of interest. Each
  # column represents a region and each row represents a time period.
  if (is.matrix(Ymat) == FALSE) {
    stop("\n No matrix supplied in 'Ymat'.\n")
  }
  # 'weightsmat' is a matrix that contains the estimated weightsmat for the synthetic
  # control unit of each region. Each columns represents a placebo region and
  # each row, a comparison unit. Pay attention that the order of the regions in
  # matrix 'weightsmat' must be identical to the order of the regions in matrix
  # 'Ymat'. 'weightsmat' is a matrix with J rows and J+1 columns, where J+1 is the
  # number of observed regions. In order to construct this matrix, one can
  # estimate a synthetic control unit using the command 'synth' (See Abadie,
  # Diamond and Hainmueller (2011).) and save the weightsmat found in the vector
  # solution.w for each region. Then, each vector solution.w will be a column
  # in matrix 'weightsmat'.
  if ((is.matrix(weightsmat) == FALSE) |
      ((dim(weightsmat)[1] != (dim(Ymat)[2] - 1)) == TRUE) |
      ((dim(weightsmat)[2] != dim(Ymat)[2]) == TRUE)) {
    stop("\n The supplid value for 'weightsmat' is invalid.\n")
  }
  # 'treated' is the column in matrix 'Ymat' associated with the treated
  # region. It must be a natural number less than or equal to the number of
  # columns in matrix 'Ymat'.
  if (((treated < 1) == TRUE) |
      ((treated > dim(Ymat)[2]) == TRUE) |
      ((treated != round(treated)) == TRUE)) {
    stop("\n The supplid value for 'treated' is invalid. \n")
  }
  # 'T0' is the row in matrix 'Ymat' associated with the last pre-intervention
  # period. It must be a natural number less than or equal to the number of
  # rows in matrix 'Ymat'.
  if (((T0 < 1) == TRUE) |
      ((T0 > dim(Ymat)[1]) == TRUE) |
      ((T0 != round(T0)) == TRUE)) {
    stop("\n The supplid value for 'T0' is invalid. \n")
  }
  # 'phi' is the sensitivity parameter defined either in step 6 or in step 7 of
  # section 3 of Firpo and Possebom (2017). It has to be a positive real number.
  # If you only want to construct the standard confidence interval under the
  # assumption that each region is equally likely to receive treatment, set
  # 'phi' to zero.
  if (phi < 0) {
    stop("\n The supplid value for 'phi' is invalid. \n")
  }
  # 'v' is the worst (best) case scenario vector defined in step 6 (step 7) of
  # section 3 of Firpo and Possebom (2017). It is row vector whose length is
  # equal to the number of observed regions, i.e., J+1. The elements of this
  # vector must be equal to 0 or 1. If you only want to construct the standard
  # confidence interval under the assumption that each region is equally likely
  # to receive treatment, let 'v' be a zero vector.
  if (((dim(v)[2] != dim(Ymat)[2]) == TRUE) |
      ((dim(v)[1] != 1) == TRUE) |
      (sum(as.numeric(v > 1)) != 0) |
      (sum(as.numeric(v < 0)) != 0)) {
    stop("\n The supplid value for 'v' is invalid. \n")
  }
  # 'precision' is a natural number. A larger value for 'precision' makes the
  # estimation of the confidence sets more precise, requiring more computing
  # time. A value between 20 and 30 is reasonable.
  if (((precision < 0) == TRUE) |
      ((precision != round(precision)) == TRUE)) {
    stop("\n The supplid value for 'precision' is invalid. \n")
  }
  # 'type' is a character vector equal to "linear", "constant" or "uniform".
  # 'type' defines which confidence subset is implemented. The "uniform"
  # confidence set was proposed by Ferman, Pinto and Possebom (2020) and
  # contains all functions that are deviations from estimated treatment effect
  # by an additive and constant factor and are not rejected by the placebo
  # test,
  if ((is.character(type) == FALSE) |
      (((type != "linear") == TRUE) & ((type != "constant") == TRUE) &
       ((type != "uniform") == TRUE))) {
    stop("\n The supplied value for 'type' is invalid. \n")
  }
  # 'significance' is the significance level in decimal form.
  if (((significance <= 0) == TRUE) |
      ((significance >= 1) == TRUE)) {
    stop("\n The supplied value for 'significance' is invalid. \n")
  }
  # 'plot' is a logical value that determines whether a plot with the
  # computed confidence set will be provided or not.
  if (is.logical(plot) == FALSE) {
    stop("\n The supplied value for 'plot' is invalid. \n")
  }
  # We define the initial parameter for the intervention effect.
  Y1 <- Ymat[, treated]
  Y0 <- Ymat[, setdiff(1:dim(Ymat)[2], treated)]
  gaps <- Y1 - Y0 %*% weightsmat[, treated]
  if (type == "constant") {
    param <- mean(gaps[(T0 + 1):dim(Ymat)[1], 1])
  } else if (type == "linear") {
    param <- gaps[dim(Ymat)[1], 1]/(dim(Ymat)[1] - T0)
  } else if (type == "uniform") {
    param <- 0
    postp <- rbind(matrix(0, T0, 1), matrix(1, dim(Ymat)[1] - T0, 1))
    nh0 <- rbind(
      matrix(0, T0, 1),
      matrix(gaps[(T0+1):dim(Ymat)[1], 1], dim(Ymat)[1] - T0, 1)
    )
  }
  s <- sign(param)
  ub <- param
  lb <- param
  # We start our loop whose iterations increase the precision of our
  # confidence sets.
  attemptu1 <- 1 # The loop has to start by not rejecting the null hypothesis.
                 # If it starts by rejecting the null hypothesis, the confidence
                 # set for the requested class of functions is empty.
                 # This parameter will help us to guarantee that.
  attemptl1 <- 1 # The loop has to start by not rejecting the null hypothesis.
                 # If it starts by rejecting the null hypothesis, the confidence
                 # set for the requested class of functions is empty.
                 # This parameter will help us to guarantee that.
  for (power in 0:precision) {
    step <- (1/2)^power
    reject.l <- 0
    reject.u <- 0
    # We start our loop whose interations try to find the upper bound
    # of our confidence set.
    while (reject.u == 0) {
      # Create the vector that represents the null hypothesis.
      if (type == "constant") {
        nh <- matrix(
          c(rep(0, T0), rep(ub, (dim(Ymat)[1] - T0))), dim(Ymat)[1], 1)
      } else if (type == "linear") {
        nh <- matrix(
          c(rep(0, T0),
            ub * seq(from = 1, to = (dim(Ymat)[1] - T0), by = 1)),
          dim(Ymat)[1], 1)
      } else if (type == "uniform") {
        nh <- nh0 + ub*postp
      }
      # Create a matrix to store the test statistics for each region.
      ts <- matrix(NA, 1, dim(Ymat)[2])
      # Estimate the gaps for each placebo unit.
      for (j in 1:dim(Ymat)[2]) {
        # Create the matrices of observed outcomes under the null.
        if (j == treated) {
          Y1 <- Ymat[, treated]
          Y0 <- Ymat[, setdiff(1:dim(Ymat)[2], treated)]
        } else {
          Y1 <- Ymat[, j] + nh
          Y0 <- Ymat[, setdiff(1:dim(Ymat)[2], j)]
          if (j < treated) {
            Y0[, (treated - 1)] <- Y0[, (treated - 1)] - nh
          } else if (j > treated) {
            Y0[, treated] <- Y0[, treated] - nh
          }
        }
        # Estimate the gaps.
        gaps <- Y1 - Y0 %*% weightsmat[, j] - nh
        # Estimate the test statistics (RMSPE).
        post <- (t(gaps[(T0 + 1):dim(Ymat)[1], 1]) %*%
                   gaps[(T0 + 1):dim(Ymat)[1], 1])/(dim(Ymat)[1] - T0)
        pre <- (t(gaps[1:T0, 1]) %*% gaps[1:T0, 1])/(T0)
        ts[1, j] <- post/pre
      }
      # Test the null hypothesis.
      rts <- matrix(NA, 1, dim(Ymat)[2])
      rts[1, ] <- rank(ts[1, ])
      prob <- exp(phi*v)/sum(exp(phi*v))
      indicator <- matrix(as.numeric(rts >= rts[1, treated]),dim(Ymat)[2],1)
      pvalue <- prob %*% indicator
      reject.u <- as.numeric(pvalue <= significance)
      if (reject.u == 0) {
        attemptu1 <- 0
        if (type == "uniform") {
          ub <- ub + step
        } else {
          ub <- param * (ub/param + s*step)
        }
      } else if (reject.u == 1) {
        if (attemptu1 == 1) {
          stop("\n The confidence set is empty for the requested class of functions. \n")
        }
        if (type == "uniform") {
          ub <- ub - step
        } else {
          ub <- param * (ub/param - s*step)
        }
      }
      if (type == "uniform") {
        if (abs(ub) > 1000000) {
          stop("\n Upper bound was not found. \n")
        }
      } else {
        if (abs(ub) > 100 * abs(param)) {
          stop("\n Upper bound was not found. \n")
        }
      }
    }
    # We start our loop whose interations try to find the lower bound
    # of our confidence set.
    while (reject.l == 0) {
      # Create the vector that represents the null hypothesis.
      if (type == "constant") {
        nh <- matrix(
          c(rep(0, T0), rep(lb, (dim(Ymat)[1] - T0))), dim(Ymat)[1], 1)
      } else if (type == "linear") {
        nh <- matrix(
          c(rep(0, T0),
            lb * seq(from = 1, to = (dim(Ymat)[1] - T0), by = 1)),
          dim(Ymat)[1], 1)
      } else if (type == "uniform") {
        nh <- nh0 + lb*postp
      }
      # Create a matrix to store the test statistics.
      ts <- matrix(NA, 1, dim(Ymat)[2])
      # Estimate the gaps for each placebo unit.
      for (j in 1:dim(Ymat)[2]) {
        # Create the matrices of observed outcomes under the null.
        if (j == treated) {
          Y1 <- Ymat[, treated]
          Y0 <- Ymat[, setdiff(1:dim(Ymat)[2], treated)]
        } else {
          Y1 <- Ymat[, j] + nh
          Y0 <- Ymat[, setdiff(1:dim(Ymat)[2], j)]
          if (j < treated) {
            Y0[, (treated - 1)] <- Y0[, (treated - 1)] - nh
          } else if (j > treated) {
            Y0[, treated] <- Y0[, treated] - nh
          }
        }
        # Estimate the gaps.
        gaps <- Y1 - Y0 %*% weightsmat[, j] - nh
        # Estimate the test statistics.
        post <- (t(gaps[(T0 + 1):dim(Ymat)[1], 1]) %*%
                   gaps[(T0 + 1):dim(Ymat)[1], 1])/(dim(Ymat)[1] - T0)
        pre <- (t(gaps[1:T0, 1]) %*% gaps[1:T0, 1])/(T0)
        ts[1, j] <- post/pre
      }
      # Test the null hypothesis.
      rts <- matrix(NA, 1, dim(Ymat)[2])
      rts[1, ] <- rank(ts[1, ])
      prob <- exp(phi*v)/sum(exp(phi*v))
      indicator <- matrix(as.numeric(rts >= rts[1, treated]),dim(Ymat)[2],1)
      pvalue <- prob %*% indicator
      reject.l <- as.numeric(pvalue <= significance)
      if (reject.l == 0) {
        attemptl1 <- 0
        if (type == "uniform") {
          lb <- lb - step
        } else {
          lb <- param * (lb/param - s*step)
        }
      } else if (reject.l == 1) {
        if (attemptl1 == 1) {
          stop("\n The confidence set is empty for the requested class of functions. \n")
        }
        if (type == "uniform") {
          lb <- lb + step
        } else {
          lb <- param * (lb/param + s*step)
        }
      }
      if (type == "uniform") {
        if (abs(lb) > 1000000) {
          stop("\n Lower bound was not found. \n")
        }
      } else {
        if (abs(ub) > 100 * abs(param)) {
          stop("\n Lower bound was not found. \n")
        }
      }
    }
  }
  # Draw the confidence interval graph.
  # Define the initial the intervention effect.
  Y1 <- Ymat[, treated]
  Y0 <- Ymat[, setdiff(1:dim(Ymat)[2], treated)]
  gaps <- Y1 - Y0 %*% weightsmat[, treated]
  # Define the upper bound of the confidence subset.
  if (type == "constant") {
    upper <- matrix(
      c(rep(NA, T0), rep(ub, (dim(Ymat)[1] - T0))), dim(Ymat)[1], 1)
  } else if (type == "linear") {
    upper <- matrix(
      c(rep(NA, T0),
        ub * seq(from = 1, to = (dim(Ymat)[1] - T0), by = 1)),
      dim(Ymat)[1], 1)
  } else if (type == "uniform") {
    upper <- nh0 + ub*postp
    upper <- matrix(
      c(rep(NA, T0), upper[(T0+1):dim(Ymat)[1], 1]),
      dim(Ymat)[1], 1
    )
  }
  # Define the lower bound of the confidence subset.
  if (type == "constant") {
    lower <- matrix(
      c(rep(NA, T0), rep(lb, (dim(Ymat)[1] - T0))), dim(Ymat)[1], 1)
  } else if (type == "linear") {
    lower <- matrix(
      c(rep(NA, T0),
        lb * seq(from = 1, to = (dim(Ymat)[1] - T0), by = 1)),
      dim(Ymat)[1], 1)
  } else if (type == "uniform") {
    lower <- nh0 + lb*postp
    lower <- matrix(
      c(rep(NA, T0), lower[(T0+1):dim(Ymat)[1], 1]),
      dim(Ymat)[1], 1
    )
  }
  # Plot all the information.
  if (plot == TRUE) {
    par(mar = c(3, 5, 1, 1))
    par(oma = c(0.1, 0.1, 0.1, 0.1))
    plot(as.matrix(1:dim(Ymat)[1], dim(Ymat)[1], 1), gaps,
         t = "n", col = "black", lwd = 2, cex = 4/5, ylim = c(-0.4,1.0),
         ylab = "Gaps",
         xlab = NA, xaxs = "i", yaxs = "i")
    polygon(x = c(as.matrix(1:dim(Ymat)[1], dim(Ymat)[1], 1),
                  rev(as.matrix(1:dim(Ymat)[1], dim(Ymat)[1], 1))),
            y = c(upper, rev(lower)), col = 'grey90', border = NA)
    lines(as.matrix(1:dim(Ymat)[1], dim(Ymat)[1], 1), gaps,
          col = "black", lwd = 2, cex = 4/5)
    abline(v = T0 + 1, lty = "dotted")
    abline(h = 0, lty = "dotted")
  }
  # The function returns the upper and lower bounds of confidence subset.
  bounds <- list(u = upper, l = lower)
  return(bounds)
}
