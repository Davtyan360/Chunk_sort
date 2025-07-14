const fs = require('fs');

const OUTPUT_FILE = 'input.txt';
const TARGET_SIZE = 0.1 * 1024 * 1024 * 1024; // 0.1GB in bytes
const CHUNK_SIZE = 100_000; // Write this count of integers per chunk

const writeStream = fs.createWriteStream(OUTPUT_FILE);

let counter = 0;
let totalBytesWritten = 0;

function writeChunk() {
    let chunk = '';
    for (let i = 0; i < CHUNK_SIZE; i++) {
        chunk += (Math.round(counter++ * Math.random())).toString() + '\n';
    }

    totalBytesWritten += Buffer.byteLength(chunk);

    const ok = writeStream.write(chunk, () => {
        process.stdout.write(`\rWritten ${(totalBytesWritten / (1024 * 1024)).toFixed(2)} MB...`);
    });

    if (totalBytesWritten < TARGET_SIZE) {
        if (!ok) {
            writeStream.once('drain', writeChunk);
        } else {
            setImmediate(writeChunk);
        }
    } else {
        writeStream.end(() => {
            console.log(`Done. File saved as ${OUTPUT_FILE} (${(totalBytesWritten / (1024 ** 3)).toFixed(2)} GB)`);
        });
    }
}

writeChunk();
