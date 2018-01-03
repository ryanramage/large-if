const DecisionTable = require('../lib/index')
const fs = require('fs')
const path = require('path')
const test = require('tape')
const csv2 = require('csv2')
const columns = ['a', 'b', 'c', 'result']

test('basic test', t => {
  let run = table => {
    table.decide(['happy', 'little', 'lots'], (err, category) => {
      t.error(err)
      t.equals(category, 'ok')
      t.end()
    })
  }
  setUp(t, path.resolve(__dirname, 'assets/table.csv'), null, run)
})

test('options - use a different wildcard', t => {
  let run = table => {
    table.decide(['happy', 'little', 'lots'], (err, category) => {
      t.error(err)
      t.equals(category, 'ok')
      t.end()
    })
  }
  setUp(t, path.resolve(__dirname, 'assets/table-wildcard.csv'), { wildcard: '#'}, run)
})

test('basic test 2', t => {
  let run = table => {
    table.decide(['sad', 'little', 'lots'], (err, category) => {
      t.error(err)
      t.equals(category, 'not-ok')
      t.end()
    })
  }
  setUp(t, path.resolve(__dirname, 'assets/table.csv'), null, run)
})

test('no matches', t => {
  let run = table => {
    table.decide(['grumpy', 'little', 'lots'], (err, category) => {
      t.ok(err)
      t.end()
    })
  }
  setUp(t, path.resolve(__dirname, 'assets/table.csv'), null, run)
})

test('too many matches', t => {
  let run = table => {
    table.decide(['a', 'b', 'c'], (err, category) => {
      t.ok(err)
      t.end()
    })
  }
  setUp(t, path.resolve(__dirname, 'assets/too-many.csv'), null, run)
})

test('more test 1', t => {
  let run = table => {
    table.decide(['sad', 'little', 'lots'], (err, category) => {
      t.error(err)
      t.equals(category, 'not-ok')
      t.end()
    })
  }
  setUp(t, path.resolve(__dirname, 'assets/table-more.csv'), null, run)
})

test('more test 2', t => {
  let run = table => {
    table.decide(['sad', 'lots', 'lots'], (err, category) => {
      t.error(err)
      t.equals(category, 'ok')
      t.end()
    })
  }
  setUp(t, path.resolve(__dirname, 'assets/table-more.csv'), null, run)
})

function setUp (t, csvLocation, opts, done) {
  let table = new DecisionTable(columns, opts)
  let ndjsonStream = fs.createReadStream(csvLocation).pipe(csv2())
  table.load(ndjsonStream, err => {
    t.error(err)
    done(table)
  })
}
