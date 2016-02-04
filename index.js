'use strict'

const _ = require('lodash')
const mysql = require('mysql')
const mysqlDbPass = require('./mysqlDbPass.json')
const mysqlUser = mysqlDbPass.user
const mysqlPwd = mysqlDbPass.pass
const apfMysql = mysql.createConnection({
  host: 'localhost',
  user: mysqlUser,
  password: mysqlPwd,
  database: 'apflora'
})
const beobMysql = mysql.createConnection({
  host: 'localhost',
  user: mysqlUser,
  password: mysqlPwd,
  database: 'apflora_beob'
})
const pg = require('pg')
const pgDbPass = require('./pgDbPass.json')
const pgUser = pgDbPass.user
const pgPwd = pgDbPass.pass
const pgConStringApflora = `postgres://${pgUser}:${pgPwd}@localhost/apflora`
const pgConStringApfloraBeob = `postgres://${pgUser}:${pgPwd}@localhost/apflora_beob`

apfMysql.connect()
beobMysql.connect()

// get a pg client from the connection pool
pg.connect(pgConStringApflora, (error, apfPg, done) => {
  if (error) {
    if (apfPg) done(apfPg)
    console.log('an error occured when trying to connect to db apflora')
  }
  console.log('connected to apflora on postgres')
  pg.connect(pgConStringApfloraBeob, (error, beobPg, done) => {
    if (error) {
      if (beobPg) done(beobPg)
      console.log('an error occured when trying to connect to db apflora_beob')
    }
    console.log('connected to apflora_beob on postgres')
    // start importing
    apfMysql.query(`SELECT * FROM apflora.adresse`, (error, results) => {
      if (error) console.log('error querying adresse from mysql')
      console.log('first result from querying adresse', results[0])
      const fields = _.keys(results[0]).map((value) => JSON.stringify(value)).join(', ')
      // const fields = _.keys(results[0]).join(',')
      console.log('fields', fields)
      results.forEach((row, index) => {
        let values = _.values(row).map((value) => JSON.stringify(value)).join(', ').replace(/"/g, "'")
        const sql = `INSERT INTO adresse (${fields}) VALUES (${values})`
        if (index < 1) console.log('sql', sql)
        apfPg.query(
          sql,
          (result) => {
            if (index < 1) console.log('result from inserting adresse in to postgres', result)
          }
        )
      })
    })
  })
})
