const fs = require('fs')
const csv2 = require('csv2')
const uuid = require('uuid/v4')
const through = require('through2')
const cartesian = require('cartesian')

module.exports = function (wildcard, clampCheck, csvLocation, done) {
  // read all the values
  let columnValues = {}
  let columnWidths = {}
  let columnHasWild = {}
  fs.createReadStream(csvLocation)
  .on('error', done)
  .pipe(csv2()).pipe(through.obj(function (chunk, enc, callback) {
    chunk.forEach((columnValue, i) => {
      if (columnValue === wildcard) {
        if (!clampCheck(i)) columnHasWild[i] = true
      } else {
        if (!columnValues[i]) columnValues[i] = {}
        columnValues[i][columnValue] = 1
        let width = columnValue.length
        if (!columnWidths[i] || width > columnWidths[i]) columnWidths[i] = width
      }
    })
    callback()
  }))

  .on('finish', () => {
    Object.keys(columnHasWild).forEach(columnName => {
      if (!columnValues[columnName]) columnValues[columnName] = {}
      columnValues[columnName][uuid()] = 1
    })
    Object.keys(columnValues).forEach(columnName => {
      columnValues[columnName] = Object.keys(columnValues[columnName])
    })
    // delete the final column. dont need it in the cartesian product
    let toDelete = `${Object.keys(columnValues).length - 1}`
    delete columnValues[toDelete]
    const allCombos = cartesian(columnValues)
    done(null, allCombos, columnWidths)
  })
}
