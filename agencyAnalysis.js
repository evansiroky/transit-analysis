const fs = require('fs')
const path = require('path')

const async = require('async')
const GTFS = require('gtfs-sequelize')
const request = require('request')
const logger = require('tracer').colorConsole()

const config = require('./config.json')

const AGENCY_DATA_DIRECTORY = './agency-data'
const MAX_CONCURRENCY = 4

/**
 * Find all agencies in California according to transitfeeds
 */
function findAgenciesOnTransitFeeds (callback) {
  logger.info('findAgenciesOnTransitFeeds')
  request({
    json: true,
    url: `https://api.transitfeeds.com/v1/getFeeds?key=${config.transitFeedsAPIKey}&location=67&descendants=1&page=1&limit=1000&type=gtfs`
  }, (err, response, body) => {
    if (err) return callback(err)
    if (body.status !== 'OK') {
      return callback(new Error(body.status))
    }
    body.results.feeds.forEach(feed => {
      feed.safeId = feed.id.replace('/', '-')
    })
    logger.info(JSON.stringify(body.results.feeds[0]))
    callback(err, body.results.feeds)
  })
}

/**
 * Create the agency data directory if it doesn't exist
 */
function createAgencyDataDirectory (callback) {
  logger.info('createAgencyDataDirectory')
  fs.mkdir(AGENCY_DATA_DIRECTORY, err => {
    if (err && err.code !== 'EEXIST') {
      return callback(err)
    }
    callback()
  })
}

/**
 * Find any directories that exist in the data directory that don't show up
 * in the transitfeeds list of agencies.  This will allow adding of manual
 * agencies with feeds that can be processed.
 */
function findManuallyCreatedAgencies (agenciesFromTransitFeeds, callback) {
  logger.info('findManuallyCreatedAgencies')
  // get list of folders in data directory
  fs.readdir(AGENCY_DATA_DIRECTORY, (err, folders) => {
    if (err) return callback(err)
    const agenciesToAdd = []
    folders.forEach(folder => {
      // check if folder name is present in list of agencies from transitfeeds
      if (!agenciesFromTransitFeeds.some(agency => agency.safeId === folder)) {
        // folder not in list of transit feeds agencies, add to list
        agenciesToAdd.push({
          safeId: folder
        })
      }
    })
    callback(null, agenciesToAdd)
  })
}

/**
 * Analyze an individual transit agency
 */
function analyzeAgency (agency, callback) {
  logger.info('analyzeAgency')
  const agencyFolder = path.join(AGENCY_DATA_DIRECTORY, agency.safeId)
  const gtfs = GTFS({
    database: 'postgres://postgres@localhost:5432/sb-827-analysis',
    downloadsDir: agencyFolder,
    gtfsFileOrFolder: 'google_transit.zip',
    gtfsUrl: agency.u ? agency.u.d : null,
    sequelizeOptions: {
      logging: false,
      schema: agency.safeId
    },
    spatial: false
  })
  async.auto(
    {
      // see if folder for agency exists
      checkFolderExistance: cb => {
        logger.info('checkFolderExistance')
        fs.stat(agencyFolder, (err, stats) => {
          if (err && err.code === 'ENOENT') {
            // folder does not exist, create it
            fs.mkdir(agencyFolder, cb)
          } else if (err) {
            // some other error
            cb(err)
          } else {
            // folder exists
            cb()
          }
        })
      },
      // see if gtfs should be downloaded
      checkForGtfs: ['checkFolderExistance', (results, cb) => {
        logger.info('checkForGtfs')
        // TODO: download most recent data
        // for now simply check if a gtfs file exists.  If not, download it
        const agencyGtfs = path.join(agencyFolder, 'google_transit.zip')
        fs.stat(agencyGtfs, (err, stats) => {
          if (err && err.code === 'ENOENT') {
            // zip file does not exist, download it
            if (!agency.u || !agency.u.d) {
              // no download link!
              logger.warn(`${agency.safeId} does not have a gtfs dl url or gtfs file!`)
              cb()
            } else {
              gtfs.downloadGtfs(cb)
            }
          } else if (err) {
            // some other error
            cb(err)
          } else {
            // zip file exists
            cb()
          }
        })
      }],
      // load gtfs into db
      loadGtfs: ['checkForGtfs', (results, cb) => {
        logger.info('loadGtfs')
        // check if schema already exists, if not, load data into db
        const db = gtfs.connectToDatabase()
        /**
         * Helper fn to create a schema and then load the data
         */
        function createSchemaAndLoad () {
          logger.info('createSchemaAndLoad')
          db.sequelize.query(`create schema if not exists "${agency.safeId}"`)
            .then(() => {
              logger.info('schema created')
              gtfs.loadGtfs(err => {
                if (err) {
                  logger.error('error loading gtfs', err)
                }
                cb(err)
              })
            })
            .catch(err => {
              logger.error(err)
              cb(err)
            })
        }
        db.route.findAll()
          .then((routes) => {
            if (routes.length > 0) {
              // some routes exist assume databse is already loaded
              cb()
            } else {
              createSchemaAndLoad()
            }
          })
          .catch(
            // assume error means db is not loaded
            createSchemaAndLoad
          )
      }],
      // find all active bus stops
      findAllActiveBusStops: ['loadGtfs', (results, cb) => {
        logger.info('findAllActiveBusStops')
        // make query for active bus stops

        // make active bus stop point geojson

        cb()
      }],
      // find all rail stops
      findAllRailStops: ['loadGtfs', (results, cb) => {
        logger.info('findAllRailStops')
        // make query for active rail stops

        // make active rail stop point geojson
        cb()
      }],
      // find all ferry stops
      findAllFerryStops: ['loadGtfs', (results, cb) => {
        logger.info('findAllFerryStops')
        // make query for active ferry stops

        // make active ferry stop point geojson
        cb()
      }]
      // TODO: bus headway calculations
      // output linestring geometry of applicable bus headway corridors
    },
    callback
  )
}

// find all gtfs's via transitfeeds
module.exports = function (callback) {
  // create queue for anlayzing agencies
  const queue = async.queue(analyzeAgency, MAX_CONCURRENCY)
  queue.drain(callback)

  // asynchronously do stuff
  async.auto(
    {
      createAgencyDataDirectory: createAgencyDataDirectory,
      findAgenciesOnTransitFeeds: findAgenciesOnTransitFeeds,
      findManuallyCreatedAgencies: [
        'createAgencyDataDirectory',
        'findAgenciesOnTransitFeeds',
        (results, cb) => {
          findManuallyCreatedAgencies(results.findAgenciesOnTransitFeeds, cb)
        }
      ],
      addTransitFeedsAgencies: [
        'findAgenciesOnTransitFeeds',
        (results, cb) => {
          // TODO: uncomment for full analysis
          queue.push(results.findAgenciesOnTransitFeeds[0])
          cb()
        }
      ],
      addManualAgencies: [
        'findManuallyCreatedAgencies',
        (results, cb) => {
          queue.push(results.findManuallyCreatedAgencies)
          cb()
        }
      ]
    },
    // do nothing on completion as that is handled by the queue drain fn
    (err) => {
      logger.info('All agencies successfully added to processing queue')
      if (err) return callback(err)
    }
  )
}
