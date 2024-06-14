import path from "node:path";
import fs from "node:fs";
import os from "node:os";

import {
    ArrowBatchCompression,
    ArrowBatchConfig, ArrowBatchConfigSchema,
    ArrowBatchReader,
    ArrowBatchWriter,
    createLogger, waitEvent
} from '../index.js';
import {randomHexString, TestChainGenerator, testDataContext} from "./utils.js";
import {expect} from "chai";

describe('reader table cache', () => {

    // test parameters
    const bucketSize = 100n;
    const dumpSize = 10n;
    const startBlock = 0n;
    const endBlock = 200n;

    const logger = createLogger('testCache', 'info');
    const tmpDataDir = path.join(os.tmpdir(), `mocha_test-cache-${randomHexString(8)}`);
    fs.mkdirSync(tmpDataDir);
    const [
        testData,
        writeRange
    ] = TestChainGenerator.genTestData(startBlock, endBlock);

    const config: ArrowBatchConfig = ArrowBatchConfigSchema.parse({
        dataDir: tmpDataDir,
        // writerLogLevel: 'debug',
        bucketSize,
        dumpSize,
    });
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
        await reader.init();

        const expectedKeys = [];

        // fill up cache by getting 1 row of each batch
        for (let i = 0n; i < 10n; i++) {
            const batchStart = i * dumpSize;
            const row = await reader.getRow(batchStart);

            expect(reader.cacheSize).to.be.equal(Number(i) + 1);

            expectedKeys.push(`0-${i}`);
        }

        // request eleventh batch should do cache trim & swap
        const lastBatchStart = 10n * dumpSize;
        await reader.getRow(lastBatchStart);
        expectedKeys.push('1-0');
        expectedKeys.shift();

        // ensure table cache size stays then same
        expect(reader.cacheSize).to.be.equal(10);

        // make sure the oldest entry was trimmed
        // @ts-ignore
        const cacheKeys = [...reader.cache.tableCache.keys()].sort();
        expect(cacheKeys).to.be.deep.equal(expectedKeys);
    });
});
