#!/usr/bin/env node
const LargeIf = require('../lib')
const generateCombos = require('../lib/generateCombos')
const async = require('async')
const fs = require('fs')
const csv2 = require('csv2')

const opts = require('rc')('large-if', {
  wildcard: '*',
  verbose: true
})
const csvLocation = opts._[0]
if (!csvLocation) usage()

generateCombos(opts.wildcard, csvLocation, (err, combos) => {
  if (err) return error(err)
  if (!combos.length) return error('No combos generated')
  let passing = []
  let errors = []
  let columns = Object.keys(combos[0])
  let table = new LargeIf(columns, opts)
  let ndjsonStream = fs.createReadStream(csvLocation).pipe(csv2())
  table.load(ndjsonStream, err => {
    if (err) return error(err)
    async.eachLimit(combos, 1, (row, cb) => {
      let asArray = Object.keys(row).map(col => row[col])
      table.decide(asArray, (err, outcome) => {
        if (err) errors.push({row, err})
        else passing.push({row, outcome})
        cb()
      })
    }, err => {
      if (err) return error(err)
      console.log('Passing combos', passing.length)
      if (opts.verbose) printPassing(passing)
      console.log('Error combos', errors.length)
      if (opts.verbose) printErrors(errors)
      if (errors.length) process.exit(1)
    })
  })

})

function printPassing (passing) {
  passing.forEach(passing => {
    let asArray = Object.keys(passing.row).map(col => passing.row[col])
    console.log(asArray, passing.outcome)
  })
}

function printErrors (errors) {
  errors.forEach(error => {
    let asArray = Object.keys(error.row).map(col => error.row[col])
    console.log(asArray, error.err.toString())
  })
}

function usage () {
  console.log('usage: large-if <table.csv>')
  console.log('this command validates a csv table to ensure it does not have logic gaps')
  process.exit(1)
}

function error (err) {
  console.log('Error: ', err.toString())
  process.exit(1)
}
