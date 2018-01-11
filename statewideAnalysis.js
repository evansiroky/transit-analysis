const fs = require('fs')
const path = require('path')

const async = require('async')
const logger = require('tracer').colorConsole()

const config = require('./config.json')
const geoUtil = require('./geo-util')

const AGENCY_DATA_DIRECTORY = config.agencyDataDirectory
const STATE_DATA_DIRECTORY = config.stateDataDirectory

const HALF_MILE = 0.5
const QUARTER_MILE = 0.25

/**
 * Create the statewide data directory if it doesn't exist
 */
function createStateDataDirectory (callback) {
  logger.info('createAgencyDataDirectory')
  fs.mkdir(STATE_DATA_DIRECTORY, err => {
    if (err && err.code !== 'EEXIST') {
      return callback(err)
    }
    callback()
  })
}

/**
 * Download the boundary of California to clip any transit service that goes
 * to different states
 */
function dlCABoundaryFromOverpass (callback) {
  geoUtil.downloadOsmBoundary(
    {
      boundaryId: 'California',
      osmQuery: {
        'ISO3166-2': 'US-CA'
      }
    },
    callback
  )
}

module.exports = function (callback) {
  logger.info('statewide analysis')
  async.auto(
    {
      createStateDataDirectory: createStateDataDirectory,
      dlCABoundaryFromOverpass: dlCABoundaryFromOverpass,
      listAgencies: cb => {
        // find all folders in agency data directory
        fs.readdir(AGENCY_DATA_DIRECTORY, (err, filesAndFolders) => {
          if (err) return cb(err)
          // don't use any non-folders (ie .DS_STORE)
          async.filter(
            filesAndFolders,
            (file, fileCallback) => {
              fs.stat(path.join(AGENCY_DATA_DIRECTORY, file), (err, stats) => {
                if (err) return fileCallback(err)
                fileCallback(null, stats.isDirectory())
              })
            },
            cb
          )
        })
      },
      makeBusStopGeojson: [
        'createStateDataDirectory',
        'dlCABoundaryFromOverpass',
        'listAgencies',
        geoUtil.makeCombineStopsFn2({
          buffers: [HALF_MILE],
          type: 'bus'
        })
      ],
      makeGoodBusHeadwayGeojson: [
        'createStateDataDirectory',
        'dlCABoundaryFromOverpass',
        'listAgencies',
        geoUtil.makeCombineStopsFn2({
          buffers: [QUARTER_MILE, HALF_MILE],
          type: 'good-headway-bus'
        })
      ],
      calculateFerryStops: [
        'createStateDataDirectory',
        'dlCABoundaryFromOverpass',
        'listAgencies',
        geoUtil.makeCombineStopsFn2({
          buffers: [QUARTER_MILE, HALF_MILE],
          type: 'ferry'
        })
      ],
      makeRailStopGeojson: [
        'createStateDataDirectory',
        'dlCABoundaryFromOverpass',
        'listAgencies',
        geoUtil.makeCombineStopsFn2({
          buffers: [QUARTER_MILE, HALF_MILE],
          type: 'rail'
        })
      ]
    },
    callback
  )
}
