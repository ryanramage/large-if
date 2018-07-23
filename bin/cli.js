#!/usr/bin/env node
const LargeIf = require('../lib')
const generateCombos = require('../lib/generateCombos')
const leftPad = require('left-pad')
const chalk = require('chalk')
const async = require('async')
const fs = require('fs')
const csv2 = require('csv2')

const opts = require('rc')('large-if', {
  wildcard: '*',
  verbose: false,
  'clamp-all': false,
  'allow-multi-match': false,
  'csv-out': false
})
if (opts['csv-out']) opts.verbose = true // make it easier for the end user
const csvLocation = opts._[0]
if (!csvLocation) usage()

let clamp = {}
if (opts.clamp) {
  opts.clamp = opts.clamp.split(',').forEach(c => { clamp[c] = true})
}
let clampCheck = (index) => {
  if (opts['clamp-all']) return true
  return clamp[index]
}

generateCombos(opts.wildcard, clampCheck, csvLocation, (err, combos, widths) => {
  if (err) return error(err)
  if (!combos.length) return error('No combos generated')
  let passing = 0
  let errors = 0
  let all = []
  let columns = Object.keys(combos[0])
  let table = new LargeIf(columns, opts)
  let ndjsonStream = fs.createReadStream(csvLocation).pipe(csv2())
  table.load(ndjsonStream, err => {
    if (err) return error(err)
    async.eachLimit(combos, 1, (row, cb) => {
      let asArray = Object.keys(row).map(col => row[col])
      table.decide(asArray, (err, outcome) => {
        if (err) {
          errors++
          all.push({row, err})
        } else {
          passing++
          all.push({row, outcome})
        }
        cb()
      })
    }, err => {
      if (err) return error(err)
      if (!opts['csv-out']) console.log('Passing combos', passing)
      if (!opts['csv-out']) console.log('Error combos', errors)
      if (opts.verbose) printAll(opts, all, widths)
      if (errors.length) process.exit(1)
    })
  })

})

function printAll (opts, all, widths) {
  let moreOutcomePadding = ''
  Object.keys(widths).forEach(w => moreOutcomePadding += leftPad('', widths[w]))
  all.forEach(d => {
    let outcome = d.outcome
    if (!Array.isArray(d.outcome)) outcome = [outcome]
    let asArray = Object.keys(d.row).map((col, i) => {
      if (opts['csv-out']) return d.row[col]
      return leftPad(d.row[col], widths[i])
    })
    if (d.err) {
      if (opts['csv-out']) console.log(d.row.join(',') + ',' + d.err.toString())
      else console.log(chalk.red(asArray.join('\t')) + `\t${d.err.toString()}`)
    } else {
      if (opts['csv-out']) console.log(asArray.join(',') + ',' + outcome.join(' '))
      else console.log(asArray.join('\t'), chalk.green(`${outcome.join(' ')}`))
    }
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
