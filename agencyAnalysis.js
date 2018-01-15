const fs = require('fs')
const path = require('path')

const async = require('async')
const GTFS = require('gtfs-sequelize')
const moment = require('moment')
const request = require('request')
const logger = require('tracer').colorConsole()

const config = require('./config.json')

const AGENCY_DATA_DIRECTORY = config.agencyDataDirectory
const ANALYSIS_DATE = '20180216'
const MAX_CONCURRENCY = 1

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
  /**
   * Helper to log a message with the agency ID
   */
  function alog (msg) {
    return `(${agency.safeId}): ${msg}`
  }

  logger.info(alog('analyzeAgency'))
  const agencyFolder = path.join(AGENCY_DATA_DIRECTORY, agency.safeId)

  const gtfs = GTFS({
    database: 'postgres://postgres@localhost:5432/sb-827-analysis',
    downloadsDir: agencyFolder,
    gtfsFileOrFolder: 'google_transit.zip',
    gtfsUrl: agency.u ? agency.u.d : null,
    interpolateStopTimes: true,
    sequelizeOptions: {
      logging: false,
      schema: agency.safeId
    },
    spatial: false
  })
  const db = gtfs.connectToDatabase()

  /**
   * Make a query of the stops of an agency by route type
   */
  function makeStopQuery (cfg) {
    let query = `
      SELECT stop.stop_id, stop.stop_lat, stop.stop_lon
      FROM "${agency.safeId}".stop stop, "${agency.safeId}".stop_time stop_time, "${agency.safeId}".trip trip, "${agency.safeId}".route route
      WHERE stop.stop_id = stop_time.stop_id
        AND stop_time.trip_id = trip.trip_id
        AND trip.route_id = route.route_id
        AND route.route_type ${cfg.routeType}
        AND trip.service_id IN (`

    query += cfg.serviceIds.map(serviceId => `'${serviceId}'`).join(',')
    query += ')'

    // logger.info(alog(query))
    return db.sequelize.query(query, { model: db.stop })
  }

  /**
   * Write a geojson file with the corresponding stops
   */
  function outputStopGeojson (type, stops, cb) {
    const geojson = {
      type: 'FeatureCollection',
      features: stops.map(stop => {
        return {
          type: 'Feature',
          properties: {
            agency: agency.safeId
          },
          geometry: {
            type: 'Point',
            coordinates: [
              stop.stop_lon,
              stop.stop_lat
            ]
          }
        }
      })
    }

    fs.writeFile(
      path.join(agencyFolder, `${type}-stop-geo.json`),
      JSON.stringify(geojson),
      cb
    )
  }

  /**
   * Helper function to run analysis on demand
   */
  function analyze () {
    async.auto(
      {
        // see if folder for agency exists
        checkFolderExistance: cb => {
          logger.info(alog('checkFolderExistance'))
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
          logger.info(alog('checkForGtfs'))
          // TODO: download most recent data
          // for now simply check if a gtfs file exists.  If not, download it
          const agencyGtfs = path.join(agencyFolder, 'google_transit.zip')
          fs.stat(agencyGtfs, (err, stats) => {
            if (err && err.code === 'ENOENT') {
              // zip file does not exist, download it
              if (!agency.u || !agency.u.d) {
                // no download link!
                logger.warn(alog(`${agency.safeId} does not have a gtfs dl url or gtfs file!`))
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
          logger.info(alog('loadGtfs'))
          // check if schema already exists, if not, load data into db

          /**
           * Helper fn to create a schema and then load the data
           */
          function createSchemaAndLoad () {
            logger.info(alog('createSchemaAndLoad'))
            db.sequelize.query(`create schema if not exists "${agency.safeId}"`)
              .then(() => {
                logger.info(alog('schema created'))
                gtfs.loadGtfs(err => {
                  if (err) {
                    logger.error(alog('error loading gtfs'))
                    logger.error(alog(err))
                  }
                  cb(err)
                })
              })
              .catch(err => {
                logger.error(alog(err))
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
        // get serviceIds for magic date
        getServiceIds: ['loadGtfs', (results, cb) => {
          const date = moment(ANALYSIS_DATE)
          const dow = date.day()
          let dowKey
          switch (dow) {
            case 0:
              dowKey = 'sunday'
              break
            case 1:
              dowKey = 'monday'
              break
            case 2:
              dowKey = 'tuesday'
              break
            case 3:
              dowKey = 'wednesday'
              break
            case 4:
              dowKey = 'thursday'
              break
            case 5:
              dowKey = 'friday'
              break
            case 6:
              dowKey = 'saturday'
              break
          }

          const applicableServiceIds = []

          /**
           * Helper function to account for a gtfs that doesn't have calendar_dates
           */
          function findInTables (calendarDatesExist) {
            const findParams = calendarDatesExist
              ? { include: [db.calendar_date] }
              : {}
            db.calendar.findAll(findParams)
              .then(calendars => {
                calendars.forEach(calendar => {
                  // determine if calendar is in range
                  if (
                    moment(calendar.start_date).isSameOrBefore(date) &&
                    moment(calendar.end_date).isSameOrAfter(date)
                  ) {
                    // get default validity
                    let isValid = calendar[dowKey] === 1

                    // check if any exception dates apply
                    if (calendarDatesExist) {
                      for (var i = 0; i < calendar.calendar_dates.length; i++) {
                        const exception = calendar.calendar_dates[i]
                        if (exception.date === ANALYSIS_DATE) {
                          // exception applies
                          if (exception.exception_type === 1) {
                            isValid = true
                          } else {
                            isValid = false
                          }
                          break
                        }
                      }
                    }

                    if (isValid) {
                      applicableServiceIds.push(calendar.service_id)
                    }
                  }
                })
                if (applicableServiceIds.length === 0) {
                  logger.warn(alog('No active schedules!'))
                }
                cb(null, applicableServiceIds)
              })
              .catch(err => {
                logger.error(alog(err))
                cb(err)
              })
          }

          // check if calendar_dates exists
          db.sequelize.query(`SELECT 1 FROM "${agency.safeId}".calendar_date LIMIT 0`)
            .then(result => {
              findInTables(true)
            })
            .catch(err => {
              if (err.message.indexOf('does not exist') > -1) {
                findInTables(false)
              } else {
                cb(err)
              }
            })
        }],
        // find all active bus stops
        findAllActiveBusStops: ['getServiceIds', (results, cb) => {
          logger.info(alog('findAllActiveBusStops'))
          if (results.getServiceIds.length === 0) {
            return outputStopGeojson('bus', [], cb)
          }
          // make query for active bus stops
          makeStopQuery({
            routeType: ' = 3',
            serviceIds: results.getServiceIds
          })
            .then(stops => {
              // make active bus stop point geojson
              outputStopGeojson('bus', stops, cb)
            })
            .catch(err => {
              logger.error(alog(err))
              cb(err)
            })
        }],
        // find all rail stops
        findAllRailStops: ['getServiceIds', (results, cb) => {
          logger.info(alog('findAllRailStops'))
          if (results.getServiceIds.length === 0) {
            return outputStopGeojson('rail', [], cb)
          }
          // make query for active rail stops
          makeStopQuery({
            routeType: ' IN (0, 1, 2)',
            serviceIds: results.getServiceIds
          })
            .then(stops => {
              // make active rail stop point geojson
              outputStopGeojson('rail', stops, cb)
            })
            .catch(err => {
              logger.error(alog(err))
              cb(err)
            })
        }],
        // find all ferry stops
        findAllFerryStops: ['getServiceIds', (results, cb) => {
          logger.info(alog('findAllFerryStops'))
          if (results.getServiceIds.length === 0) {
            return outputStopGeojson('ferry', [], cb)
          }
          // make query for active ferry stops
          makeStopQuery({
            routeType: ' = 4',
            serviceIds: results.getServiceIds
          })
            .then(stops => {
              // make active ferry stop point geojson
              outputStopGeojson('ferry', stops, cb)
            })
            .catch(err => {
              logger.error(alog(err))
              cb(err)
            })
        }],
        // bus headway calculations
        calculateBusHeadways: ['getServiceIds', (results, cb) => {
          logger.info(alog('calculateBusHeadways'))
          if (results.getServiceIds.length === 0) {
            return outputStopGeojson('good-headway-bus', [], cb)
          }
          const serviceIdSql = results.getServiceIds.map(serviceId => `'${serviceId}'`).join(',')
          const directions = [0, 1]

          /**
           * Calculate all stops that satisfy headway requirements
           * use a very strict interpretation of bus corridors to make it easier
           * to produce some output.  Only assume that the 15 minute criteria
           * excludes interlined routes.  All trips must be from the same route
           * and have the same direction id to count.  Furthermore, the segment
           * that a route travels does not count, a radius will be drawn only
           * from each stop that is served at least every 15 minutes, the parts
           * between stops don't count.  Also, peak hours are assumed to be 6am to 9am
           * and 3pm to 6pm.
           */
          function getRouteDirectionStopTimes (cfg, stopTimeCallback) {
            let query = `
              SELECT stop.stop_id, stop.stop_lat, stop.stop_lon,
                stop_time.arrival_time
              FROM "${agency.safeId}".stop stop, "${agency.safeId}".stop_time stop_time, "${agency.safeId}".trip trip
              WHERE stop.stop_id = stop_time.stop_id
                AND stop_time.trip_id = trip.trip_id
                AND trip.route_id = '${cfg.routeId}'
                AND trip.direction_id = ${cfg.directionId}
                AND trip.service_id IN (${serviceIdSql})
                AND stop_time.arrival_time >= ${cfg.beginTime}
                AND stop_time.arrival_time <= ${cfg.endTime}
              ORDER BY stop.stop_id, stop_time.arrival_time ASC`

            // logger.info(alog(query))
            db.sequelize.query(query)
              .then(rows => {
                if (rows[0].length === 0) {
                  return stopTimeCallback(null, {})
                }

                // got rows, calculate headways at each stop
                const peakStopsWith15MinHeadways = {}
                let curStop = {}
                let maxHeadway
                let lastArrivalTime

                /**
                 * Helper to check on stop stats to calculate if it met
                 * headway requirement
                 */
                function resolveLastStopTimeForStop () {
                  maxHeadway = Math.max(maxHeadway, cfg.endTime - curStop.arrival_time)
                  if (maxHeadway <= 900) {
                    peakStopsWith15MinHeadways[curStop.stop_id] = curStop
                  }
                }

                rows[0].forEach(row => {
                  // logger.info(alog(row))
                  if (row.stop_id !== curStop.stop_id) {
                    if (curStop.stop_id) {
                      resolveLastStopTimeForStop()
                    }
                    curStop = row
                    maxHeadway = curStop.arrival_time - cfg.beginTime
                  } else {
                    curStop = row
                    maxHeadway = Math.max(maxHeadway, curStop.arrival_time - lastArrivalTime)
                  }
                  lastArrivalTime = curStop.arrival_time
                })
                resolveLastStopTimeForStop()
                stopTimeCallback(null, peakStopsWith15MinHeadways)
              })
              .catch(err => {
                logger.error(alog(err))
                stopTimeCallback(err)
              })
          }

          /**
           * Calculate the stop_times with at least a 15 minute headway
           * Headways must be at most 15 minutes during both peak periods
           */
          function calculateRouteDirectionHeadways (cfg, routeDirectionCallback) {
            async.auto(
              {
                calculateMorningPeakStopTimes: morningPeakCallback => {
                  getRouteDirectionStopTimes(
                    {
                      beginTime: 21600, // 6am
                      endTime: 32400, // 9am
                      directionId: cfg.directionId,
                      routeId: cfg.routeId
                    },
                    morningPeakCallback
                  )
                },
                calculateAfternoonPeakStopTimes: afternoonPeakCallback => {
                  getRouteDirectionStopTimes(
                    {
                      beginTime: 54000, // 3pm
                      endTime: 64800, // 6pm
                      directionId: cfg.directionId,
                      routeId: cfg.routeId
                    },
                    afternoonPeakCallback
                  )
                },
                calculateStopsWithGoodHeadwaysInBothPeaks: [
                  'calculateMorningPeakStopTimes',
                  'calculateAfternoonPeakStopTimes',
                  (results, bothPeaksCallback) => {
                    Object.keys(results.calculateMorningPeakStopTimes).forEach(stopId => {
                      if (results.calculateAfternoonPeakStopTimes[stopId]) {
                        stopsWith15MinHeadways[stopId] = results.calculateAfternoonPeakStopTimes[stopId]
                      }
                    })
                    bothPeaksCallback()
                  }
                ]
              },
              routeDirectionCallback
            )
          }

          const routeDirectionCalcQueue = async.queue(calculateRouteDirectionHeadways, MAX_CONCURRENCY)

          const stopsWith15MinHeadways = {}

          db.route.findAll({
            where: {
              route_type: 3
            }
          })
            .then(routes => {
              if (routes.length === 0) {
                return outputStopGeojson('good-headway-bus', [], cb)
              }
              routes.forEach(route => {
                directions.forEach(directionId => {
                  routeDirectionCalcQueue.push({
                    directionId: directionId,
                    routeId: route.route_id
                  })
                })
              })

              routeDirectionCalcQueue.drain = () => {
                logger.info(alog('done calculating good frequency stops'))
                outputStopGeojson(
                  'good-headway-bus',
                  Object.keys(stopsWith15MinHeadways).map(
                    key => stopsWith15MinHeadways[key]
                  ),
                  cb
                )
              }
            })
            .catch(err => {
              logger.error(alog(err))
              cb(err)
            })
        }]
      },
      err => {
        db.sequelize.close()
        callback(err)
      }
    )
  }

  // check if output files have already been generated
  // if so assume already complete from earlier run
  fs.stat(path.join(agencyFolder, 'bus-stop-geo.json'), (err, stats) => {
    if (err) {
      if (err.code === 'ENOENT') {
        // file does not exist, perform analysis
        analyze()
      } else if (err.code === 'ENOTDIR') {
        // not a directory, skip
        logger.info(alog('not a directory, skipping calculation'))
        callback()
      } else {
        // some other error
        logger.error(alog(err))
        callback(err)
      }
    } else {
      // output exists, assume no need to recalculate
      logger.info(alog('output exists, skipping calculation'))
      callback()
    }
  })
}

// find all gtfs's via transitfeeds
module.exports = function (callback) {
  // create queue for anlayzing agencies
  const queue = async.queue(analyzeAgency, MAX_CONCURRENCY)

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
          queue.push(results.findAgenciesOnTransitFeeds)
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
      queue.drain = () => {
        logger.info('Queue drain')
        callback()
      }
      if (err) {
        logger.error(err)
        return callback(err)
      }
    }
  )
}
