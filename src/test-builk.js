const https = require('https')

const body = `{ "index": { "_index": "test_index", "_id": "tt1979320" } },
{ "title": "Rush", "year": 2013 },
{ "create": { "_index": "test_index", "_id": "tt1392214" } },
{ "title": "Prisoners", "year": 2013 }`;
//const AUTH = Buffer.from('testuser:$TestPWD0').toString('base64')
const AUTH = "dGVzdHVzZXI6JFRlc3RQV0QwCg=="
console.log(AUTH)
 const p = new Promise((resolve, reject) => {
        const options = {
          host: 'search-leakeddb-66rpqjlt22x7gs4wrkiiqo75ju.us-west-1.es.amazonaws.com',
          path : `/test-index/_bulk`,
          method:'POST',
          port:443,
          headers : {
              'Content-Type' : 'application/x-ndjson',
              'Content-Length': Buffer.byteLength(JSON.stringify(body)),
              'Authorization' :`Basic: ${AUTH}`,
              'region' : 'us-west-1',
          }
      };
      const req = https.request(options, (res) => {
          if (res.statusCode < 200 || res.statusCode >= 300){
              return reject(new Error('statusCode=' + res.statusCode));
          }
          var resbody = [];
          res.on('data', (chunk) => {
              resbody.push(chunk);
          });
          res.on('end', () => {
              resolve(resbody);
          });
          return null;
      });
      req.on('error', (e) =>{
          console.log("We got an error from post request",e);
          reject(e.message);
      });
      req.write(JSON.stringify(body));
      req.end();
    });

Promise.all([p]).then(console.log).catch(console.error);

