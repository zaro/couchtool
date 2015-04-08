#!/usr/bin/env coffee

nano = require('nano')
yargs = require('yargs')
fs = require('fs')

class CouchTool
	constructor:(@dbUrl)->
		@db = nano(@dbUrl)
	runCmd: (cmd, file)->
		if typeof @[cmd] is 'function'
			@[cmd](file)
		else
			throw new Error('Invalid command ', cmd)
	restore: (file)->
		new Promise (resolve, reject)=>
			docs = JSON.parse( fs.readFileSync(file) )
			console.log 'Read ', docs.docs.length, 'documents from ', file
			@db.bulk docs, (err, body)->
				if err
					console.error 'Restore failed:', err
					reject(err)
				else
					console.log body
					resolve()
	dump: (file)->
		new Promise (resolve, reject)=>
			@db.list {include_docs:true},(err, body)=>
				if err
					console.error 'Failed to dump db documents', err
					reject(err)
					return
				docs = []
				for row in body.rows
					docs.push( row.doc )
				fs.writeFile file , JSON.stringify({docs:docs}), (err)->
					if err
						console.error 'Failed to write', file, err
						reject(err)
					else
						console.log 'Written', docs.length, 'documents to', file ,'.'
						resolve()

argv = require('yargs')
	.usage('Usage: $0 <command> <DB_URL> <FILE>')
	.demand(3)
	.argv;

ct = new CouchTool(argv._[1])
ct.runCmd(argv._[0], argv._[2]).then ()->
	console.log 'All done.'
.catch (err)->
	console.error err
