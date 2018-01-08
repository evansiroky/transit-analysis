const async = require('async')
const logger = require('tracer').colorConsole()

const agencyAnalysis = require('./agencyAnalysis')
const statewideAnalysis = require('./statewideAnalysis')

async.series(
  [
    agencyAnalysis,
    statewideAnalysis
  ],
  (err, results) => {
    if (err) {
      logger.error('error: ', err)
    }
    logger.info('completed without error')
  }
)
