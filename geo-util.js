const exec = require('child_process').exec
const fs = require('fs')
const path = require('path')

const async = require('async')
const jsts = require('jsts')
const overpass = require('query-overpass')
const rimraf = require('rimraf')
const logger = require('tracer').colorConsole()

const config = require('./config.json')

const AGENCY_DATA_DIRECTORY = config.agencyDataDirectory
const STATE_DATA_DIRECTORY = config.stateDataDirectory

var geoJsonReader = new jsts.io.GeoJSONReader()
var geoJsonWriter = new jsts.io.GeoJSONWriter()
const lowestAcceptablePrcesion = 1000000
var precisionModel = new jsts.geom.PrecisionModel(lowestAcceptablePrcesion)
var precisionReducer = new jsts.precision.GeometryPrecisionReducer(precisionModel)

/**
 * Copied from timezone-boundary-builder, forget what this does
 */
function fetchIfNeeded (file, superCallback, downloadCallback, fetchFn) {
  // check for file that got downloaded
  fs.stat(file, function (err) {
    if (!err) {
      // file found, skip download steps
      return superCallback(null, geoJsonToGeom(require(file)))
    }
    // check for manual file that got fixed and needs validation
    var fixedFile = file.replace('.json', '_fixed.json')
    fs.stat(fixedFile, function (err) {
      if (!err) {
        // file found, return fixed file
        return downloadCallback(null, require(fixedFile))
      }
      // no manual fixed file found, download from overpass
      fetchFn()
    })
  })
}

function geoJsonToGeom (geoJson) {
  try {
    return geoJsonReader.read(JSON.stringify(geoJson))
  } catch (e) {
    console.error('error converting geojson to geometry')
    fs.writeFileSync('debug_geojson_read_error.json', JSON.stringify(geoJson))
    throw e
  }
}

function geomToGeoJson (geom) {
  return geoJsonWriter.write(geom)
}

function geomToGeoJsonString (geom) {
  return JSON.stringify(geoJsonWriter.write(geom))
}

var minRequestGap = 4
var curRequestGap = 4
const millisecondsPerSecond = 1000

/**
 * Perform a geographic operation on two geometries
 */
function debugGeo (op, a, b, reducePrecision) {
  var result

  if (reducePrecision) {
    a = precisionReducer.reduce(a)
    b = precisionReducer.reduce(b)
  }

  try {
    switch (op) {
      case 'union':
        result = a.union(b)
        break
      case 'intersection':
        result = a.intersection(b)
        break
      case 'intersects':
        result = a.intersects(b)
        break
      case 'diff':
        result = a.difference(b)
        break
      default:
        var err = new Error('invalid op: ' + op)
        throw err
    }
  } catch (e) {
    if (e.name === 'TopologyException') {
      logger.warn('Encountered TopologyException, retry with GeometryPrecisionReducer')
      return debugGeo(op, a, b, true)
    }
    logger.error('op err')
    logger.error(e)
    logger.error(e.stack)
    fs.writeFileSync('debug_' + op + '_a.json', JSON.stringify(geoJsonWriter.write(a)))
    fs.writeFileSync('debug_' + op + '_b.json', JSON.stringify(geoJsonWriter.write(b)))
    throw e
  }

  return result
}

function downloadOsmBoundary (cfg, boundaryCallback) {
  var query = '[out:json][timeout:60];(relation'
  var boundaryFilename = './' + path.join(STATE_DATA_DIRECTORY, cfg.boundaryId + '.json')
  var debug = 'getting data for ' + cfg.boundaryId
  var queryKeys = Object.keys(cfg.osmQuery)

  for (var i = queryKeys.length - 1; i >= 0; i--) {
    var k = queryKeys[i]
    var v = cfg.osmQuery[k]

    query += '["' + k + '"="' + v + '"]'
  }

  query += ');out body;>;out meta qt;'

  logger.info(debug)
  logger.info(query)

  async.auto({
    downloadFromOverpass: function (cb) {
      logger.info('downloading from overpass')
      fetchIfNeeded(boundaryFilename, boundaryCallback, cb, function () {
        function overpassResponseHandler (err, data) {
          if (err) {
            logger.warn(err)
            logger.warn('Increasing overpass request gap')
            curRequestGap *= 2
            makeQuery()
          } else {
            logger.info('Success, decreasing overpass request gap')
            curRequestGap = Math.max(minRequestGap, curRequestGap / 2)
            cb(null, data)
          }
        }
        function makeQuery () {
          logger.info('waiting ' + curRequestGap + ' seconds')
          setTimeout(function () {
            overpass(query, overpassResponseHandler, { flatProperties: true })
          }, curRequestGap * millisecondsPerSecond)
        }
        makeQuery()
      })
    },
    validateOverpassResult: ['downloadFromOverpass', function (results, cb) {
      var data = results.downloadFromOverpass
      if (!data.features || data.features.length === 0) {
        var err = new Error('Invalid geojson for boundary: ' + cfg.boundaryId)
        return cb(err)
      }
      cb()
    }],
    saveSingleMultiPolygon: ['validateOverpassResult', function (results, cb) {
      var data = results.downloadFromOverpass
      var combined

      // union all multi-polygons / polygons into one
      for (var i = data.features.length - 1; i >= 0; i--) {
        var curOsmGeom = data.features[i].geometry
        if (curOsmGeom.type === 'Polygon' || curOsmGeom.type === 'MultiPolygon') {
          logger.info('combining border')
          try {
            var curGeom = geoJsonToGeom(curOsmGeom)
          } catch (e) {
            console.error('error converting overpass result to geojson')
            console.error(e)
            fs.writeFileSync(cfg.boundaryId + '_convert_to_geom_error.json', JSON.stringify(curOsmGeom))
            fs.writeFileSync(cfg.boundaryId + '_convert_to_geom_error-all-features.json', JSON.stringify(data))
            throw e
          }
          if (!combined) {
            combined = curGeom
          } else {
            combined = debugGeo('union', curGeom, combined)
          }
        }
      }
      try {
        fs.writeFile(boundaryFilename, geomToGeoJsonString(combined), err => cb(err, combined))
      } catch (e) {
        console.error('error writing combined border to geojson')
        fs.writeFileSync(cfg.boundaryId + '_combined_border_convert_to_geom_error.json', JSON.stringify(data))
        throw e
      }
    }]
  }, (err, results) => {
    boundaryCallback(err, results.saveSingleMultiPolygon)
  })
}

/**
 * Combine all stops of a certain type from each agency into one file
 */
function makeCombineStopsFn (cfg) {
  return function (callback) {
    logger.info(`combine ${cfg.type} stops`)
    const geojson = {
      type: 'FeatureCollection',
      features: []
    }
    cfg.agencies.forEach(agency => {
      const agencyStops = require(
        './' + path.join(
          AGENCY_DATA_DIRECTORY,
          agency,
          cfg.type + '-stop-geo.json'
        )
      )

      // hacky thing: only add stops within CA for agencies with known stops outside CA
      if (agency === 'amtrak') {
        agencyStops.features.forEach(stop => {
          const stopGeom = geoJsonToGeom(stop.geometry)
          if (cfg.caBoundary.contains(stopGeom)) {
            geojson.features.push(stop)
          }
        })
      } else {
        geojson.features = geojson.features.concat(agencyStops.features)
      }
    })

    // write geojson to file
    const stopGeojsonPath = path.join(STATE_DATA_DIRECTORY, `${cfg.type}-stop-geo.json`)
    fs.writeFile(
      stopGeojsonPath,
      JSON.stringify(geojson),
      (err) => {
        if (err) return callback(err)
        // create shapefile
        const stopShapefilePath = path.join(STATE_DATA_DIRECTORY, `${cfg.type}-stop.shp`)
        // remove old shapefile so ogr2ogr can work
        rimraf(stopShapefilePath, err => {
          if (err) return callback(err)
          exec(
            `ogr2ogr -nlt POINT "${stopShapefilePath}" "${stopGeojsonPath}" OGRGeoJSON`,
            (err, stdout, stderr) => {
              if (err) return callback(err)
              callback(null, geojson)
            }
          )
        })
      }
    )
  }
}

const MILES_TO_BUFFER_DEGREES = 69.047

// buffering in jsts takes forever, don't do
/**
 * Buffer all stops by a certain buffer amount and write them to a
 * geojson file and a shapefile
 */
function makeBufferStopsFn (cfg) {
  return [
    'combineStops',
    (results, callback) => {
      logger.info(`buffer ${cfg.type} stops (${cfg.buffer})`)
      const geometryFactory = new jsts.geom.GeometryFactory()
      const stopGeoms = results.combineStops.features.map(feature =>
        geoJsonToGeom(feature.geometry)
      )
      const stopGeomCollection = geometryFactory.createGeometryCollection(stopGeoms)
      const bufferedStops = stopGeomCollection.buffer(cfg.buffer / MILES_TO_BUFFER_DEGREES)

      // write buffered stops to file
      const fileName = `${cfg.type}-buffered-by-${cfg.buffer}`
      const bufferedStopGeojsonPath = path.join(STATE_DATA_DIRECTORY, `${fileName}.json`)
      fs.writeFile(
        bufferedStopGeojsonPath,
        JSON.stringify(geomToGeoJson(bufferedStops)),
        err => {
          if (err) return callback(err)
          // create shapefile
          const bufferedStopShapefilePath = path.join(STATE_DATA_DIRECTORY, `${fileName}.shp`)
          // remove old shapefile so ogr2ogr can work
          rimraf(bufferedStopShapefilePath, err => {
            if (err) return callback(err)
            exec(
              `ogr2ogr -nlt POLYGON "${bufferedStopShapefilePath}" "${bufferedStopGeojsonPath}" OGRGeoJSON`,
              (err, stdout, stderr) => {
                if (err) {
                  logger.error(err)
                  return callback(err)
                }
                callback(null, bufferedStops)
              }
            )
          })
        }
      )
    }
  ]
}

/**
 * Combine and buffer stops from all agency outputs
 */
function makeCombineAndBufferStopsFn (cfg) {
  return function (results, callback) {
    const autoCfg = {
      // combine all stops into one geojson file of points
      combineStops: makeCombineStopsFn({
        agencies: results.listAgencies,
        caBoundary: results.dlCABoundaryFromOverpass,
        type: cfg.type
      })
    }
    cfg.buffers.forEach((buffer, idx) => {
      autoCfg[`buffer${idx}`] = makeBufferStopsFn({
        buffer: buffer,
        type: cfg.type
      })
    })
    async.auto(autoCfg, callback)
  }
}

/**
 * Cause it's 1am and I want to get this shit done and go to bed
 */
function makeCombineStopsFn2 (cfg) {
  return function (results, callback) {
    makeCombineStopsFn({
      agencies: results.listAgencies,
      caBoundary: results.dlCABoundaryFromOverpass,
      type: cfg.type
    })(callback)
  }
}

module.exports = {
  downloadOsmBoundary: downloadOsmBoundary,
  makeCombineStopsFn2: makeCombineStopsFn2,
  makeCombineAndBufferStopsFn: makeCombineAndBufferStopsFn
}
