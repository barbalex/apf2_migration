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
    // get list of all tables
    const sql = `SELECT
            table_schema || '.' || table_name
          FROM
              information_schema.tables
          WHERE
              table_type = 'BASE TABLE'
          AND
              table_schema NOT IN ('pg_catalog', 'information_schema');`
    apfPg.query(sql, (error, result) => {
      const tablesWithSchema = result.rows.map((row) => row['?column?'])
      const tables = tablesWithSchema.map((table) => table.replace('public.', ''))
      console.log('tables', tables)
      tables.forEach((table) => {
        // start importing
        apfMysql.query(`SELECT * FROM apflora.${table}`, (error, results) => {
          if (error) console.log('error querying adresse from mysql')
          // need to join field names, enclosed in double quotes
          const fields = _.keys(results[0]).map((value) => JSON.stringify(value)).join(', ')
          console.log('fields', fields)
          results.forEach((row, index) => {
            // need to join values, strings enclosed in single quotes
            const values = _.values(row).map((value) => JSON.stringify(value).replace(/\'/, '`').replace(/^"/g, "'").replace(/"$/g, "'")).join(', ')
            const sql = `INSERT INTO ${table} (${fields}) VALUES (${values})`
            apfPg.query(sql, (error) => {
              if (error) {
                console.log('error', error)
                console.log('at row', row)
              }
            })
          })
        })
      })
    })
  })
})
