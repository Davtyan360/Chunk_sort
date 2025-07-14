const { parentPort, workerData } = require('worker_threads');
const fs = require('fs');

const { chunkPath } = workerData;

(async () => {
  const numbers = fs.readFileSync(chunkPath, 'utf-8')
    .split('\n')
    .filter(Boolean)
    .map(Number)
    .sort((a, b) => a - b);

  fs.writeFileSync(chunkPath, numbers.join('\n'), 'utf-8');
  console.log(`${chunkPath} was sorted`);
  
  parentPort.postMessage({ status: 'sorted', path: chunkPath });
})();
