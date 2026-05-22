'use strict';

const { DNSEClient } = require('../dnse');

async function main() {
  const client = new DNSEClient({
    apiKey: 'replace-with-api-key',
    apiSecret: 'replace-with-api-secret',
    baseUrl: 'https://openapi.dnse.com.vn',
  });

  const { status, body } = await client.getOhlc('STOCK', {
    query: {
      symbol: 'HPG',
      resolution: '1',
      from: 1735689600,
      to: 1735776000,
    },
    dryRun: false,
  });
  console.log(status, body);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
