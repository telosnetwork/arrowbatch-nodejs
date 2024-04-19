import path from "node:path";
import fs from "node:fs";
import os from "node:os";

import {
    ArrowBatchCompression,
    ArrowBatchConfig,
    ArrowBatchReader,
    ArrowBatchWriter,
    createLogger, RowWithRefs
} from '../index.js';
import {randomHexString, TestChainGenerator, testDataContext, waitEvent} from "./utils.js";
import {expect} from "chai";

describe('read/write', () => {

    // test parameters
    const bucketSize = 100n;
    const dumpSize = 10n;
    const startBlock = 0n;
    const endBlock = 20n;

    const logger = createLogger('testWriter', 'info');
    const tmpDataDir = path.join(os.tmpdir(), `mocha_test-writer-${randomHexString(8)}`);
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

    // delete data dir after tests
    after(() => {
        fs.rmSync(config.dataDir, { recursive: true });
    });

    const readRange = async (from: number, to: number): Promise<RowWithRefs[]> => {
        const reader = new ArrowBatchReader(config, testDataContext, logger);
        await reader.init(startBlock);

        const blocks = [];

        for (let i = BigInt(from); i <= BigInt(to); i++) {
            const row = await reader.getRow(i);
            blocks.push(row);
        }

        return blocks;
    };

    beforeEach(async () => {
        writer = new ArrowBatchWriter(config, testDataContext, logger);
        await writer.init(startBlock);
    });

    afterEach(async () => {
        await writer.deinit();
    });

    const halfBatch = Number(dumpSize / 2n);

    it('write first batch', async () => {
        const batchStart = Number(startBlock);
        const batchEnd = Number(startBlock + dumpSize - 1n);

        // writer.init should create the datadir and dump the data definitions
        expect(fs.existsSync(writer.config.dataDir)).to.be.true;
        expect(fs.existsSync(path.join(writer.config.dataDir, 'context.json'))).to.be.true;

        // write full batch, should automatically trigger flush
        writeRange(writer, batchStart, batchEnd);
        await waitEvent(writer.events, 'flush');

        // wip bucket should be created & blocks file be present
        expect(fs.existsSync(writer.wipBucketPath)).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'block.ab'))).to.be.true;

        // no wip blocks file should exist, we wrote full batch
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'block.ab.wip'))).to.be.false;

        // no blocks should remain on RAM buffers
        expect(writer.intermediateSize).to.be.equal(0);
        expect(writer.auxiliarySize).to.be.equal(0);

        // read all blocks and deep compare, should match
        expect(
            await readRange(batchStart, batchEnd)
        ).to.be.deep.equal(
            testData.slice(batchStart, batchEnd + 1)
        );
    });

    it('half write second batch, leave unfinished', async () => {
        const prevBatchEnd = Number(startBlock + dumpSize) - 1;
        const batchStart = prevBatchEnd + 1;
        const batchHalf = batchStart + halfBatch;

        // after init from wip data, last ord should match prev tests end
        expect(writer.lastOrdinal).to.be.equal(BigInt(prevBatchEnd));

        // write half batch
        writeRange(writer,  batchStart, batchHalf);

        // blocks should be in RAM buffers
        expect(writer.intermediateSize).to.be.equal(batchHalf - prevBatchEnd);
        expect(writer.auxiliarySize).to.be.equal(0);

        // explictly flush
        writer.beginFlush();
        await waitEvent(writer.events, 'flush');

        // wip bucket, block file with first batch & block wip file should be present
        expect(fs.existsSync(writer.wipBucketPath)).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'block.ab'))).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'block.ab.wip'))).to.be.true;

        // no blocks should remain on RAM buffers
        expect(writer.intermediateSize).to.be.equal(0);
        expect(writer.auxiliarySize).to.be.equal(0);

        // read all blocks and deep compare, should match
        expect(
            await readRange(Number(startBlock), batchHalf - 1)
        ).to.be.deep.equal(
            testData.slice(Number(startBlock), batchHalf)
        );
    });

    it('finish second batch', async () => {
        const prevBatchEnd = Number(startBlock + dumpSize) + halfBatch;
        const batchStart = prevBatchEnd + 1;
        const batchEnd = batchStart + halfBatch - 2;

        // after init from wip data, last ord should match prev tests end
        expect(writer.lastOrdinal).to.be.equal(BigInt(prevBatchEnd));

        // finish batch, should automatically trigger flush
        writeRange(writer,  batchStart, batchEnd);
        await waitEvent(writer.events, 'flush');

        // wip bucket should be created & blocks file be present
        expect(fs.existsSync(writer.wipBucketPath)).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'block.ab'))).to.be.true;

        // no wip blocks file should exist, we wrote full batch
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'block.ab.wip'))).to.be.false;

        // no blocks should remain on RAM buffers
        expect(writer.intermediateSize).to.be.equal(0);
        expect(writer.auxiliarySize).to.be.equal(0);

        // read all blocks and deep compare, should match
        expect(
            await readRange(Number(startBlock), batchEnd)
        ).to.be.deep.equal(
            testData.slice(Number(startBlock), batchEnd + 1)
        );
    });
});
