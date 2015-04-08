#!/usr/bin/env coffee

nano = require('nano')
yargs = require('yargs')
fs = require('fs-extra')
path = require('path')

# rework to use  https://github.com/dominictarr/JSONStream

class CouchTool
	constructor:(@dbUrl)->
		if typeof @dbUrl is 'string'
			nanoConfig = {
				url: @dbUrl
			}
		else
			nanoConfig = @dbUrl
		@db = nano(nanoConfig)
	runCmd: (cmd, file)->
		if typeof @[cmd] is 'function'
			@[cmd](file)
		else
			throw new Error('Invalid command ', cmd)
	loadAttachments: (dir, doc)->
		return unless doc._attachments
		docDir = dir + '/' + doc._id
		for name, md of doc._attachments
			doc._attachments[name].data = fs.readFileSync(docDir + '/' + name).toString('base64')
			doc._attachments[name].stub = false
		null
	restore: (dir)->
		file = dir + '/docs.json'
		new Promise (resolve, reject)=>
			docs = null
			revs = {}
			Promise.all([
				new Promise (resolve, reject)->
					fs.readFile file, (err, data)->
						if err
							reject(err)
							return
						docs = JSON.parse(data)
						console.log 'Read ', docs.docs.length, 'documents from ', file
						resolve()
				new Promise (resolve, reject)=>
					@db.list {include_docs:true}, (err, body)=>
						if err
							reject(err)
							return
						for row in body.rows
							revs[row.id] = row.value.rev
						console.log 'DB has', Object.keys(revs).length, 'documents '
						resolve()
			]).then ()=>
				for doc in docs.docs
					@loadAttachments(dir, doc)
					if revs[doc._id]
						doc._rev = revs[doc._id]
					else
						delete doc._rev
				@db.bulk docs, (err, body)->
					if err
						console.error 'Restore failed:', err
						reject(err)
					else
						okCount = 0
						errs = []
						for res in body
							if res.ok
								okCount++
							else
								errs.push(res)
						console.log "Inserted/Replaced #{ okCount } documents successfully"
						if errs.length
							console.error "Failed:", errs
						resolve()
	dumpAttachments: (dir, doc)->
		return unless doc._attachments
		docDir = dir + '/' + doc._id
		for name, md of doc._attachments
			file = docDir + '/' + name
			fs.ensureDirSync(path.dirname(file))
			@db.attachment.get(doc._id, name).pipe(fs.createWriteStream(file))
		null
	dump: (dir)->
		fs.ensureDirSync(dir)
		file = dir + '/docs.json'
		new Promise (resolve, reject)=>
			@db.list {include_docs:true}, (err, body)=>
				if err
					console.error 'Failed to dump db documents', err
					reject(err)
					return
				docs = []
				for row in body.rows
					docs.push( row.doc )
					@dumpAttachments(dir, row.doc)
				fs.writeFile file , JSON.stringify({docs:docs},null, '\t'), (err)->
					if err
						console.error 'Failed to write', file, err
						reject(err)
					else
						console.log 'Written', docs.length, 'documents to', file ,'.'
						resolve()

argv = require('yargs')
	.usage('Usage: $0 <command> <DB_URL> <FILE>')
	.demand(3)
	.boolean('insecure')
	.default('insecure', false)
	.alias('k', 'insecure')
	.argv;

ct = new CouchTool({url: argv._[1], requestDefaults:{ strictSSL: !argv.insecure } })
ct.runCmd(argv._[0], argv._[2]).then ()->
	console.log 'All done.'
.catch (err)->
	console.error err
