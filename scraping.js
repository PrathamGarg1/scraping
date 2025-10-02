import fs from 'fs';
import { promisify } from 'util';
import { setTimeout } from 'timers/promises';
import { HttpsProxyAgent } from 'https-proxy-agent';
import pLimit from 'p-limit';
import fetch from 'node-fetch';

const writeFile = promisify(fs.writeFile);
const readFile = promisify(fs.readFile);
const exists = promisify(fs.exists);

// Configuration
const CONFIG = {
  totalPages: 4166,
  outputFile: './all_docs.json',
  batchSize: 10, // Number of parallel requests
  retries: 3,    // Number of retries per request
  delay: 100,    // Delay between batches in ms
  timeout: 30000, // Request timeout in ms
};

const headers = {
  'accept': 'application/json, text/javascript, */*; q=0.01',
  'accept-language': 'en-US,en;q=0.9',
  'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
  'x-requested-with': 'XMLHttpRequest',
  'cookie': '_gid=GA1.3.896192862.1759297212; _ga_MMQ7TYBESB=GS2.1.s1759387623$o4$g1$t1759388602$j60$l0$h0; _ga=GA1.1.1762965010.1759297212; csrf_gem_cookie=bb297e3d56cd11e2f4153157604330b4; ci_session=ee4f5792fdd7e469e5cf5dee1dc8f34db4f3eab3; GeM=1458192740.20480.0000; TS0123c430=01e393167d27e0e5fe5f8a99d2eb694e8b04d81c0ea2168afa938137b42530ece2756c5dce5a8c8082e1b7398b61776596b250ae970da857cbd322d073f38ae30d00550684bded2608dd8da8e15f8ff94998b4e3cd5c7a454dc1fab1340cb56a2d1eb7fb33',
  'Referer': 'https://bidplus.gem.gov.in/all-bids'
};

const CSRF_TOKEN = 'bb297e3d56cd11e2f4153157604330b4';

// Rate limiter
const limit = pLimit(CONFIG.batchSize);

// Check if file exists and load progress
async function loadProgress() {
  if (await exists(CONFIG.outputFile)) {
    const data = await readFile(CONFIG.outputFile, 'utf8');
    return JSON.parse(data);
  }
  return [];
}

// Save progress
async function saveProgress(data) {
  await writeFile(CONFIG.outputFile, JSON.stringify(data, null, 2));
}

// Fetch a single page with retries
async function fetchPage(page, retryCount = 0) {
  const payload = {
    param: { searchBid: '', searchType: 'fullText' },
    filter: {
      bidStatusType: 'ongoing_bids',
      byType: 'all',
      highBidValue: '',
      byEndDate: { from: '', to: '' },
      sort: 'Bid-End-Date-Oldest'
    },
    page
  };

  const body = `payload=${encodeURIComponent(JSON.stringify(payload))}&csrf_bd_gem_nk=${CSRF_TOKEN}`;

  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(CONFIG.timeout).then(() => {
      controller.abort();
      throw new Error('Request timeout');
    });

    const response = await Promise.race([
      fetch('https://bidplus.gem.gov.in/all-bids-data', {
        method: 'POST',
        headers,
        body,
        signal: controller.signal,
        agent: process.env.HTTP_PROXY ? new HttpsProxyAgent(process.env.HTTP_PROXY) : undefined,
      }),
      timeoutId
    ]);

    clearTimeout(timeoutId);

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return data?.response?.response?.docs || [];
  } catch (error) {
    if (retryCount < CONFIG.retries) {
      console.warn(`Retry ${retryCount + 1} for page ${page}:`, error.message);
      await setTimeout(1000 * (retryCount + 1)); // Exponential backoff
      return fetchPage(page, retryCount + 1);
    }
    console.error(`Failed to fetch page ${page} after ${CONFIG.retries} retries:`, error);
    return [];
  }
}

// Process a batch of pages
async function processBatch(startPage, endPage, allDocs) {
  const pages = Array.from({ length: endPage - startPage + 1 }, (_, i) => startPage + i);
  
  const results = await Promise.all(
    pages.map(page => 
      limit(async () => {
        const docs = await fetchPage(page);
        console.log(`✅ Page ${page}: Fetched ${docs.length} docs`);
        return { page, docs };
      })
    )
  );

  // Merge results
  for (const { page, docs } of results) {
    allDocs = allDocs.concat(docs);
  }

  // Save progress after each batch
  await saveProgress(allDocs);
  console.log(`💾 Saved batch ${startPage}-${endPage}. Total docs: ${allDocs.length}`);
  
  return allDocs;
}

// Main function
async function scrape() {
  try {
    console.log('🚀 Starting GEM bid scraper...');
    
    // Load existing data if any
    let allDocs = await loadProgress();
    const processedPages = new Set(allDocs.map(doc => doc.page));
    
    console.log(`📊 Resuming with ${allDocs.length} existing documents`);

    // Process in batches
    for (let page = 1; page <= CONFIG.totalPages; page += CONFIG.batchSize) {
      const endPage = Math.min(page + CONFIG.batchSize - 1, CONFIG.totalPages);
      
      // Skip already processed pages in this batch
      const pagesToProcess = [];
      for (let p = page; p <= endPage; p++) {
        if (!processedPages.has(p)) {
          pagesToProcess.push(p);
        }
      }
      
      if (pagesToProcess.length === 0) {
        console.log(`⏩ Skipping batch ${page}-${endPage} (already processed)`);
        continue;
      }

      console.log(`\n🔄 Processing batch ${pagesToProcess[0]}-${pagesToProcess[pagesToProcess.length - 1]}/${CONFIG.totalPages}`);
      
      allDocs = await processBatch(
        pagesToProcess[0],
        pagesToProcess[pagesToProcess.length - 1],
        allDocs
      );
      
      // Add delay between batches
      if (page + CONFIG.batchSize <= CONFIG.totalPages) {
        await setTimeout(CONFIG.delay);
      }
    }

    console.log(`\n🎉 Scraping completed! Total documents: ${allDocs.length}`);
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
}

// Run the scraper
scrape();
