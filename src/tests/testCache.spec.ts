import path from "node:path";
import fs from "node:fs";
import os from "node:os";

import {
    ArrowBatchCompression,
    ArrowBatchConfig,
    ArrowBatchReader,
    ArrowBatchWriter,
    createLogger
} from '../index.js';
import {randomHexString, TestChainGenerator, testDataContext, waitEvent} from "./utils.js";
import {expect} from "chai";

describe('reader table cache', () => {

    // test parameters
    const bucketSize = 100n;
    const dumpSize = 10n;
    const startBlock = 0n;
    const endBlock = 200n;

    const logger = createLogger('testCache', 'info');
    const tmpDataDir = path.join(os.tmpdir(), `mocha_test-cache-${randomHexString(8)}`);
    const [
        testData,
        writeRange
    ] = TestChainGenerator.genTestData(startBlock, endBlock);

    const config: ArrowBatchConfig = {
        dataDir: tmpDataDir,
        compression: ArrowBatchCompression.ZSTD,
        writerLogLevel: 'info',
        bucketSize,
        dumpSize,
    };
    let writer: ArrowBatchWriter;

    beforeEach(async () => {
        writer = new ArrowBatchWriter(config, testDataContext, logger);
        await writer.init(startBlock);
    });

    afterEach(async () => {
        await writer.deinit();
    });

    // delete data dir after tests
    after(() => {
        fs.rmSync(config.dataDir, { recursive: true });
    });

    it('cache trimming when full', async () => {
        // write 10 batches
        for (let i = 0; i < 11; i++) {
            const batchStart = i * Number(dumpSize);
            const batchEnd = ((i + 1) * Number(dumpSize)) - 1;
            writeRange(writer,  batchStart, batchEnd);
            await waitEvent(writer.events, 'flush');
        }

        const reader = new ArrowBatchReader(config, testDataContext, logger);
        await reader.init(startBlock);

        const expectedKeys = [];

        // fill up cache by getting 1 row of each batch
        for (let i = 0n; i < 10n; i++) {
            const batchStart = i * dumpSize;
            await reader.getRow(batchStart);

            expect(reader.cacheSize).to.be.equal(Number(i) + 1);

            expectedKeys.push(`0-${i}`);
        }

        // request eleventh batch should do cache trim & swap
        const lastBatchStart = 10n * dumpSize;
        await reader.getRow(lastBatchStart);
        expectedKeys.push(`1-0`);
        expectedKeys.shift();

        // ensure table cache size stays then same
        expect(reader.cacheSize).to.be.equal(10);

        // make sure the oldest entry was trimmed
        // @ts-ignore
        const cacheKeys = [...reader.cache.tableCache.keys()];
        expect(cacheKeys).to.be.deep.equal(expectedKeys);
    });

    it('cache metadata update when table on disk change', async () => {
        const lastBatchStart = 10n * dumpSize;

        // open reader and read last batch into cache
        const reader = new ArrowBatchReader(config, testDataContext, logger);
        await reader.init(startBlock);
        await reader.getRow(lastBatchStart);

        expect(reader.cacheSize).to.be.equal(1);
        // @ts-ignore
        const firstMetaTs = reader.cache.metadataCache.get(1).ts;

        // add a byte at end of table file to trigger meta update
        fs.appendFileSync(
            writer.tableFileMap.get(1).get('root'),
            'test'
        );

        // re-read same row
        await reader.getRow(lastBatchStart);

        expect(reader.cacheSize).to.be.equal(1);
        // @ts-ignore
        const secondMetaTs = reader.cache.metadataCache.get(1).ts;

        expect(secondMetaTs).to.be.greaterThan(firstMetaTs);
    });
});
