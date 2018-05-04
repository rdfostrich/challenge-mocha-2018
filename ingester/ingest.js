const util = require('util');
const ostrich = require('ostrich-bindings');
const parser = require('n3').Parser();
const fs = require('fs');

ingest(process.argv[2], parseInt(process.argv[3], 10), process.argv[4]);

async function ingest(path, version, baseDir) {
    const store = await openStore(path);
    const start = process.hrtime();
    const insertedCount = await ingestVersion(store, version, baseDir);
    const end = process.hrtime(start);
    const millis = (end[0] * 1000 + end[1].toFixed(3) / 1000000).toFixed(0);
    console.error('Duration: ' + millis + "ms");
    store.close();
    console.log(insertedCount + "," + millis);
}

function openStore(path) {
    return new Promise((resolve, reject) => {
        ostrich.fromPath(path, false, function (error, store) {
            if (error) {
                reject(error);
            } else {
                resolve(store);
            }
        });
    });
}

function ingestVersion(store, version, baseDir) {
    return new Promise(async (resolve, reject) => {
        store.append(version, await readVersion(version, baseDir), (error, insertedCount) => {
            if (error) {
                reject(error);
            } else {
                console.error('Inserted: ' + insertedCount);
                resolve(insertedCount);
            }
        });
    });
}

function readVersion(version, baseDir) {
    return new Promise(async (resolve, reject) => {
        const files = await util.promisify(fs.readdir)(baseDir);
        const triples = [];
        for (let file of files) {
            file = baseDir + file;
            if (file.endsWith('deleted.nt')) {
                await readTriples(triples, file, false);
            } else if (file.endsWith('.nt')) {
                await readTriples(triples, file, true);
            }
        }
        resolve(triples);
    });
}

function readTriples(triples, file, additions) {
    return new Promise((resolve, reject) => {
        parser.parse(fs.createReadStream(file), (error, triple, prefixes) => {
            if (error) {
                reject(error);
            } else if (triple) {
                triple.addition = additions;
                triples.push(triple);
            } else {
               resolve();
            }
        });
    });
}
