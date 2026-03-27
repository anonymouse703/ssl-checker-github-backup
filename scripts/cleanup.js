const fs = require('fs');
const path = require('path');

const RESULTS_DIR = path.join('C:/ssl-checker-tool', 'results');
const OUTPUT_DIR = path.join('C:/ssl-checker-tool', 'output');

function cleanupOldResults(daysOld = 30) {
  console.log(`🧹 Cleaning up results older than ${daysOld} days...`);
  
  const now = Date.now();
  const cutoff = now - (daysOld * 24 * 60 * 60 * 1000);
  
  if (fs.existsSync(RESULTS_DIR)) {
    const years = fs.readdirSync(RESULTS_DIR);
    
    years.forEach(year => {
      const yearPath = path.join(RESULTS_DIR, year);
      if (fs.statSync(yearPath).isDirectory()) {
        const months = fs.readdirSync(yearPath);
        
        months.forEach(month => {
          const monthPath = path.join(yearPath, month);
          if (fs.statSync(monthPath).isDirectory()) {
            const batches = fs.readdirSync(monthPath);
            
            batches.forEach(batch => {
              const batchPath = path.join(monthPath, batch);
              const stats = fs.statSync(batchPath);
              
              if (stats.mtimeMs < cutoff) {
                console.log(`   🗑️  Removing: ${year}/${month}/${batch}`);
                fs.rmSync(batchPath, { recursive: true, force: true });
              }
            });
          }
        });
      }
    });
  }
  
  console.log('✅ Cleanup complete!');
}

// Run if called directly
if (require.main === module) {
  const days = process.argv[2] ? parseInt(process.argv[2]) : 30;
  cleanupOldResults(days);
}

module.exports = { cleanupOldResults };