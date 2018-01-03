const addDoc = require('../lib/addDoc')
const test = require('tape')

test('erroring docDb', t => {
  let docDb = { put: (id, val, cb) => cb(new Error('error')) }
  let columnDb = { put: (id, val, cb) => cb(new Error('error')) }
  addDoc({}, [], docDb, columnDb, {}, err => {
    t.ok(err)
    t.end()
  })
})
