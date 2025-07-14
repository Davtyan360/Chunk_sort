const fs = require('fs');
const os = require('os');
const path = require('path');
const readline = require('readline');
const { Worker } = require('worker_threads');

const INPUT_FILE = 'input.txt';
const OUTPUT_FILE = 'sorted-output.txt';
const CHUNK_SIZE_MB = 10;
const CHUNK_SIZE_BYTES = CHUNK_SIZE_MB * 1024 * 1024;
const CHUNKS_DIR = './chunks';

let chunkCount = 0;
const chunkPaths = [];



async function splitFileIntoChunks() {
    if (!fs.existsSync(CHUNKS_DIR)) {
        fs.mkdirSync(CHUNKS_DIR);
    }


  return new Promise((resolve) => {
    const rl = readline.createInterface({
      input: fs.createReadStream(INPUT_FILE),
      crlfDelay: Infinity,
    });

    let chunkLines = [];
    let currentSize = 0;

    rl.on('line', (line) => {
      chunkLines.push(line);
      currentSize += Buffer.byteLength(line + '\n');        
      if (currentSize >= CHUNK_SIZE_BYTES) {
        const chunkPath = path.join(CHUNKS_DIR, `chunk_${chunkCount++}.txt`);
        console.log(chunkPath);
        
        fs.writeFileSync(chunkPath, chunkLines.join('\n') + '\n');
        chunkPaths.push(chunkPath);
        chunkLines = [];
        currentSize = 0;
      }
    });

    rl.on('close', () => {
      if (chunkLines.length > 0) {
        const chunkPath = path.join(CHUNKS_DIR, `chunk_${chunkCount++}.txt`);
        fs.writeFileSync(chunkPath, chunkLines.join('\n') + '\n');
        chunkPaths.push(chunkPath);
      }
      resolve();
    });
  });
}

function sortChunkWithWorker(chunkPath) {
  return new Promise((resolve, reject) => {
    const worker = new Worker('./worker.js', {
      workerData: { chunkPath }
    });

    worker.on('message', (msg) => {
      if (msg.status === 'sorted') resolve(msg.path);
    });

    worker.on('error', reject);
    worker.on('exit', (code) => {
      if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
    });
  });
}

async function sortAllChunksInParallel() {
  const maxThreads = Math.min(os.cpus().length, chunkPaths.length);  
  for (let i = 0; i < chunkPaths.length; i += maxThreads) {
    const batch = chunkPaths.slice(i, i + maxThreads);
    await Promise.all(batch.map(sortChunkWithWorker));    
  }
}

async function mergeSortedChunks(outputFile = OUTPUT_FILE) {
    if (chunkPaths.length === 0) {
      console.log('No chunk files found to merge.');
      return;
    }
    
    console.log(`Merging ${chunkPaths.length} chunk files...`);
    
    const readers = chunkPaths.map(file => readline.createInterface({
      input: fs.createReadStream(file),
      crlfDelay: Infinity
    })[Symbol.asyncIterator]());
  
    const currentValues = new Array(readers.length);
    const doneFlags = new Array(readers.length).fill(false);
  
    const outputStream = fs.createWriteStream(outputFile);
  
    for (let i = 0; i < readers.length; i++) {
      const { value, done } = await readers[i].next();
      if (!done) {
        currentValues[i] = parseInt(value, 10);
      } else {
        doneFlags[i] = true;
      }
    }
  
    while (true) {
      let minIndex = -1;
      let minValue = Infinity;
  
      for (let i = 0; i < currentValues.length; i++) {
        if (!doneFlags[i] && currentValues[i] < minValue) {
          minValue = currentValues[i];
          minIndex = i;
        }
      }
  
      if (minIndex === -1) break;
  
      outputStream.write(currentValues[minIndex] + '\n');
  
      const { value, done } = await readers[minIndex].next();
      if (!done) {
        currentValues[minIndex] = parseInt(value, 10);
      } else {
        doneFlags[minIndex] = true;
      }
    }
  
    outputStream.end();
    console.log(`Final sorted output written to ${outputFile}`);
  }

async function mergeExistingChunks(outputFile = OUTPUT_FILE) {
  console.log('Starting merge of existing chunk files');
  
  if (chunkPaths.length === 0) {
    console.log('No chunk files found to merge.');
    return false;
  }
  
  console.log(`Found ${chunkPaths.length} chunk files to merge`);
  
  for (const chunkFile of chunkPaths) {
    fs.readFileSync(chunkFile, 'utf-8').split('\n').slice(0, 10).filter(Boolean);
  }
  
  await mergeSortedChunks(outputFile);
  return true;
}


(async () => {
  console.log('Splitting file...');
  await splitFileIntoChunks();

  console.log('Sorting chunks in parallel...');
  await sortAllChunksInParallel();

  console.log('Merging sorted chunks...');
  await mergeSortedChunks();

})();
