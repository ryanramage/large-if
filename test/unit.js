const DecisionTable = require('../lib/index')
const fs = require('fs')
const path = require('path')
const test = require('tape')
const csv = require('csv-parser')
const csv2 = require('csv2')
const JSONStream = require('JSONStream')
const columns = ['a', 'b', 'c', 'result']

const csvLocation = path.resolve(__dirname, 'table.csv')

test('basic test', function (t) {
  let table = new DecisionTable(columns)
  let ndjsonStream = fs.createReadStream(csvLocation)
    .pipe(csv2())
  table.load(ndjsonStream, err => {
    t.error(err)
    table.decide(['green', 'little', 'lots'], (err, category) => {
      t.error(err)
      console.log(category)
      t.equals(category, 'ok')
      t.ok(true)
      t.end()
    })
  })
})
