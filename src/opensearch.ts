import * as fs from 'fs'
import { Client } from '@opensearch-project/opensearch'
import yargs from 'yargs'
import * as csv from 'csv-stream'

const argv = yargs
    .command('index', 'Index csv document', {
        'node': {
            description: 'Node URL',
            type: 'string',
        },
        'delete-index': {
            description: 'Deleting index',
            type: 'boolean',
            default: false
        },
        'doc': {
            description: 'Path to CSV document to index',
            type: 'string'
        },
        'id': {
            description: 'Name of CSV field to be an _id',
            type: 'string'
        },
        'limit-bytes': {
            description: 'Limiting processing to N bytes',
            type: 'number',
            default: 0
        },
        'bulk-size': {
            description: 'Number of items in bulk',
            type: 'number',
            default: 4096
        },
        'index-name': {
            description: 'Name of the index',
            type: 'string'
        },
        'columns': {
            description: 'List of column names',
            type: 'array'
        },
        'column-offset': {
            description: 'List of column names',
            type: 'number',
            default: 0,
        },
        'ignore': {
            description: 'List of columns to ignore',
            type: 'array',
            default: []
        },
        'delimiter': {
            description: 'Column delimiter',
            type: 'string',
            default: '\t'
        },
        'end-line': {
            description: 'Row delimiter',
            type: 'string',
            default: '\n'
        },
        'escape-char': {
            description: 'Character to escape',
            type: 'string',
            default: ''
        },
        'enclosed-char': {
            description: 'Character to enclose',
            type: 'string',
            default: ''
        },
        'verbose': {
            description: 'Verbose output',
            type: 'boolean',
            default: false
        }

    })
    .help()
    .alias('help', 'h').argv;

const node = argv['node']
const ssl = {
    //ca: fs.readFileSync('./root-ca.pem')
    rejectUnauthorized: false
}
// Initialize the client.
const client = new Client({
    node,
    ssl,
    suggestCompression: true
})

console.log(node)

//process.exit(0);


async function index(url: string) {
    const _index = argv['indexName']

    console.log("Index:" + _index)
    // All of these arguments are optional.
    var options = {
        delimiter: argv["delimiter"], // default is ,
        endLine: argv["endLine"], // default is \n,
        columns: argv["columns"], // default read the first line and use values found as columns
        columnOffset: argv["columnOffset"], // default is 0
        escapeChar: argv["escapeChar"], // default is an empty string
        enclosedChar: argv["enclosedChar"], // default is an empty string
    }

    if (argv['delete-index']) {
        console.log('Deleting index', _index);
        await client.indices.delete({ index: _index }).then(console.log).catch(console.error)
    }

    console.log(options)
    const csvStream = csv.createStream(options)
    let idx = 0, arr = []

    const readable = fs.createReadStream(url
        , { end: argv['limit-bytes'] ? argv['limit-bytes'] : undefined }
    ).pipe(csvStream)


    const indexRecords = async (): Promise<void | any[]> => {
        const operations = arr.flatMap(doc => [{
            create: {
                _index,
                _id: argv['id'] ? doc[argv['id']] : undefined
            }
        }, doc])

        if (argv['verbose'])
            console.log(operations)

        return client.bulk({
            refresh: true,
            body: operations
        })
            .then((response) => {
                console.log(`Indexed ${idx} records:`,
                    response.statusCode === 200 ? 'Success' : response
                )
                return {
                    response
                }
            })
            //.then(({ response }) => new Promise(resolve => setTimeout(() => resolve(response), 5000)))
            .then(() => arr.splice(0, arr.length))
            .catch(err => console.error(err))
    }


    let cancelled = false
    readable
        .on('error', (err: any) => {
            console.error(err);
        })
        .on('header', (columns: any[]) => {
            console.log('Columns', columns);
        })
        .on('data', async (data: any) => {
            if (cancelled) return
            // outputs an object containing a set of key/value pair representing a line found in the csv file.
            arr.push(data)
            if ((++idx % argv['bulk-size']) === 0) {
                readable.pause();
                await indexRecords()
                    .then(() => readable.resume())
            } else {
                //readable.resume()
            }
        })
        .on('end', async () => {
            console.log(`Stream ended at ${idx} line position`)
            return await indexRecords()
                .then(() => console.log('Final records indexed'))
                .then(() => client.close())
        })
}
index(argv['doc']).then(console.log)

