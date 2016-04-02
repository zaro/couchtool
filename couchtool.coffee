#!/usr/bin/env coffee

yargs = require('yargs')
fs = require('fs-extra')
path = require('path')
zlib = require('zlib')
url=require('url')
request = require('request')
JSONStream = require('JSONStream')
filesize = require('filesize')

class FileWriter
	constructor: (filePattern, cfg={})->
		@filePattern = filePattern ? 'docs.#/'
		@maxDoc = cfg.maxDoc ? 1000
		@buckets = cfg.buckets ? 11
		@gzip = cfg.gzip ? true

	bucketForName: (name)->
		hash = 0
		for  i in [0...name.length]
    	hash += name.charCodeAt(i)
		return hash % @buckets

	getFilename: ( name)->
		pattern = @filePattern
		pattern.replace('#', @bucketForName(name)) + (name ? '')

	push: (doc, name)->
			fileName = @getFilename(name)
			fs.ensureDirSync(path.dirname(fileName))
			data = JSON.stringify(doc)
			if @gzip
				data = zlib.gzipSync(data)
			fs.writeFileSync(fileName, data)

	saveAttachment: (file, requestObject)->
		fs.ensureDirSync(path.dirname(file))
		if @gzip
			gzip = zlib.createGzip()
			dataStream = requestObject.pipe(gzip)
		else
			dataStream = requestObject
		requestObject.pipe(fs.createWriteStream(file))

class FileReader
	constructor: (filePattern, cfg={})->
		@filePattern = filePattern ? 'docs.#'
		@maxDoc = cfg.maxDoc ? 1000
		@docsFilenames = []
		@buckets = cfg.buckets ? 11
		@gzip = cfg.gzip ? true

	load: (dir)->
		pattern = new RegExp( @filePattern.replace('#','\\d+') )
		files = []
		for d in fs.readdirSync(dir)
			continue unless d.match(pattern)
			docDir = dir + '/' + d
			files = files.concat(docDir + '/' + f for f in fs.readdirSync(docDir))
		@docsFilenames = files

	pop: (numFiles=@maxDoc)->
		fileArray = []
		while numFiles > 0 && @docsFilenames.length > 0
			numFiles -= 1
			file = @docsFilenames.shift()
			data = fs.readFileSync(file)
			if @gzip
				data = zlib.gunzipSync(data)
			doc = JSON.parse(data)
			if doc._attachments
				for name, md of doc._attachments
					doc._attachments[name].data = @loadAttachment(file, doc, name)
					doc._attachments[name].stub = false
			fileArray.push(doc)
		fileArray
	loadAttachment: (file, doc, name)->
		attFile = path.dirname(file)  + '/_attachments/'  + encodeURIComponent(doc.id) + '/' + encodeURIComponent(name)
		att = fs.readFileSync(attFile)
		if @gzip
			data = zlib.gunzipSync(data)
		data.toString('base64')

class CouchTool
	constructor:(@cfg)->
		@dbUrl = @cfg.url
		if @dbUrl[@dbUrl.length-1] != '/'
			@dbUrl += '/'
	dbRequest:(opts={}, callback)->
		unless opts.url
			opts.url = @dbUrl
		unless opts.url.match(/https?:\/\//)
			opts.url = @dbUrl + opts.url
		opts.json = true
		opts.strictSSL = !@cfg.opts.insecure
		request(opts, callback)
	runCmd: (cmd, dir)->
		if not dir
			u = url.parse(@dbUrl)
			paths = u.pathname.split('/')
			while paths.length
				dir = paths.pop()
				if dir
					break
			if not dir
				dir = 'couchdbdump'
		if typeof @[cmd] is 'function'
			@[cmd](dir)
		else
			throw new Error('Invalid command ', cmd)
	info: ()->
		new Promise (resolve, reject)=>
			@dbRequest {}, (error, response, body)=>
				if error
					reject(error)
				console.log 'Database name:       ', body.db_name
				console.log 'Number of documents: ', body.doc_count
				console.log 'Size on disk:        ', filesize(body.disk_size)
				console.log 'Data size:           ', filesize(body.data_size)
				resolve()
	buckets: ()->
		new Promise (resolve, reject)=>
			@dbRequest {}, (error, response, body)=>
			reader = @dbRequest({url:'_all_docs'})
			reader = reader.pipe(JSONStream.parse('rows.*'))
			out = new FileWriter(null, {buckets:@cfg.opts.buckets})
			buckets = (0 for i in [0...@cfg.opts.buckets])
			reader.on 'data', (row) =>
				b = out.bucketForName(row.id)
				buckets[b] += 1
			reader.on 'end', ()=>
				for i in [0...buckets.length]
					console.log i + ':\t'  + buckets[i]
				resolve()
			reader.on 'error', (err)=>
				reject(err)
	restore: (dir)->
		requests = []
		getRevReq = new Promise (resolve, reject)=>
			console.log 'Getting current document revisions...'
			reader = @dbRequest({url:'_all_docs'})
			reader = reader.pipe(JSONStream.parse('rows.*'))
			revs = {}
			reader.on 'data', (row) =>
				revs[row.id] = row.value.rev
			reader.on 'end', ()=>
				resolve(revs)
			reader.on 'error', (err)=>
				reject(err)
		getRevReq.then (revisions)=>
			fin = new FileReader('docs.#', @cfg.opts)
			console.log 'Loading documents...'
			fin.load(dir)
			while true
				batch = fin.pop()
				break unless batch.length
				for doc in batch
					if revisions[doc._id]
						doc['_rev'] = revisions[doc._id]
					else
						delete doc['_rev']
				body = { docs: batch }
				do (body)=>
					requests.push new Promise (resolve, reject)=>
						@dbRequest {url:'_bulk_docs', method: 'POST', body}, (error, response, body)=>
							if error
								reject(error)
							#console.log body
							hasError = false
							for row in body
								if row.error
									console.error row.id + '\t' + row.reason
									hasError = true
							if hasError
								reject('')
							else
								resolve()
		requests.push getRevReq
		Promise.all(requests)
	dump: (dir)->
		fs.ensureDirSync(dir)
		new Promise (resolve, reject) =>
			console.log 'Dumping documents...'
			out = new FileWriter(dir + '/docs.#/', @cfg.opts)
			reader = @dbRequest({url:'_all_docs', qs:{include_docs:true}})
			reader = reader.pipe(JSONStream.parse('rows.*'))
			reader.on 'data', (row) =>
				file = encodeURIComponent(row.id)
				doc = row.doc
				#console.log doc._id
				out.push(doc, file)
				if doc._attachments
					attDir = out.getFilename('/_attachments/')
					for name, md of doc._attachments
						file = attDir + '/' + encodeURIComponent(doc._id) + '/' + encodeURIComponent(name)
						out.saveAttachment(file, @dbRequest({url:doc._id + '/' + name}))
			reader.on 'end', ()=>
				resolve()
			reader.on 'error', (err)=>
				reject(err)



argv = require('yargs')
	.usage('Usage: $0 <COMMAND> <DATABASE_URL> [DIRECTORY]')
	.demand(2)
	.command('info', 'Show basic database info')
	.command('dump', 'Dump databse to disk')
	.command('restore', 'Restore databse from disk')
	.command('buckets', 'Show how many docs will be in each dir on disk')
	.boolean('insecure')
	.describe('insecure', "Allow insecure SSL")
	.default('insecure', false)
	.alias('k', 'insecure')
	.boolean('gzip')
	.describe('gzip', "Gzip docs and attachments")
	.default('gzip', false)
	.alias('z', 'gzip')
	.describe('buckets', "Numbers of dirs to split docs in")
	.default('buckets', 10)
	.alias('b', 'buckets')
	.argv;

ct = new CouchTool({url: argv._[1], opts: argv })
ct.runCmd(argv._[0], argv._[2])
.then ()->
	console.log 'All done.'
.catch (err)->
	console.error err if err
	console.error err.stack if err.stack
	console.log 'Failed.'
