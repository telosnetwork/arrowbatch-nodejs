import path from "node:path";
import fs from "node:fs";
import os from "node:os";

import {
    ArrowBatchCompression,
    ArrowBatchConfig,
    ArrowBatchReader,
    ArrowBatchWriter,
    createLogger, RowWithRefs, waitEvent
} from '../index.js';
import {randomHexString, TestChainGenerator, testDataContext} from "./utils.js";
import {expect} from "chai";

describe('read/write', () => {

    // test parameters
    const bucketSize = 100n;
    const dumpSize = 10n;
    const startBlock = 0n;
    const endBlock = 200n;

    const logger = createLogger('testWriter', 'info');
    const tmpDataDir = path.join(os.tmpdir(), `mocha_test-rw-${randomHexString(8)}`);
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

        for await (const row of reader.iter({
            from: BigInt(from), to: BigInt(to)
        }))
            blocks.push(row);

        return blocks;
    };

    const compareRange = async (from: number, to: number) => {
        const read = await readRange(from, to);
        const actual = testData.slice(from, to + 1);

        // for extra debugging
        // for (let i = 0; i < read.length; i++) {
        //     try {
        //         expect(read[i]).to.be.deep.equal(actual[i]);
        //     } catch (e) {
        //         console.log(i);
        //         throw e;
        //     }
        // }

        expect(read).to.be.deep.equal(actual);
    }

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
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx.ab'))).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx_log.ab'))).to.be.true;

        // no wip blocks file should exist, we wrote full batch
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'block.ab.wip'))).to.be.false;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx.ab.wip'))).to.be.false;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx_log.ab.wip'))).to.be.false;

        // no blocks should remain on RAM buffers
        expect(writer.intermediateSize).to.be.equal(0);
        expect(writer.auxiliarySize).to.be.equal(0);

        // read all blocks and deep compare, should match
        await compareRange(Number(startBlock), batchEnd);
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
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx.ab'))).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx_log.ab'))).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'block.ab.wip'))).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx.ab.wip'))).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx_log.ab.wip'))).to.be.true;

        // no blocks should remain on RAM buffers
        expect(writer.intermediateSize).to.be.equal(0);
        expect(writer.auxiliarySize).to.be.equal(0);

        // read all blocks and deep compare, should match
        await compareRange(Number(startBlock), batchHalf - 1);
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
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx.ab'))).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx_log.ab'))).to.be.true;

        // no wip blocks file should exist, we wrote full batch
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'block.ab.wip'))).to.be.false;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx.ab.wip'))).to.be.false;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx_log.ab.wip'))).to.be.false;

        // no blocks should remain on RAM buffers
        expect(writer.intermediateSize).to.be.equal(0);
        expect(writer.auxiliarySize).to.be.equal(0);

        // read all blocks and deep compare, should match
        await compareRange(Number(startBlock), batchEnd);
    });

    it('write full bucket', async () => {
        const prevBatchEnd = Number(startBlock + (dumpSize * 2n)) - 1;
        const startWipBucket = writer.wipBucketPath;

        // after init from wip data, last ord should match prev tests end
        expect(writer.lastOrdinal).to.be.equal(BigInt(prevBatchEnd));

        for (let i = 2; i < Number(endBlock / dumpSize); i++) {
            const batchStart = i * Number(dumpSize);
            const batchEnd = ((i + 1) * Number(dumpSize)) - 1;
            writeRange(writer,  batchStart, batchEnd);
            await waitEvent(writer.events, 'flush');
        }

        // after writing up to endBlock - 1 wip bucket should have changed
        expect(writer.wipBucketPath).to.not.be.equal(startWipBucket);

        // prev bucket should be 100% done
        const startBucket = startWipBucket.replace('.wip', '');
        expect(fs.existsSync(startBucket)).to.be.true;
        expect(fs.existsSync(path.join(startBucket, 'block.ab'))).to.be.true;
        expect(fs.existsSync(path.join(startBucket, 'tx.ab'))).to.be.true;
        expect(fs.existsSync(path.join(startBucket, 'tx_log.ab'))).to.be.true;

        expect(fs.existsSync(path.join(startBucket, 'block.ab.wip'))).to.be.false;
        expect(fs.existsSync(path.join(startBucket, 'tx.ab.wip'))).to.be.false;
        expect(fs.existsSync(path.join(startBucket, 'tx_log.ab.wip'))).to.be.false;

        // current wip bucket should have all table files
        expect(fs.existsSync(writer.wipBucketPath)).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'block.ab'))).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx.ab'))).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx_log.ab'))).to.be.true;

        expect(fs.existsSync(path.join(writer.wipBucketPath, 'block.ab.wip'))).to.be.false;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx.ab.wip'))).to.be.false;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx_log.ab.wip'))).to.be.false;

        expect(writer.lastOrdinal).to.be.equal(BigInt(endBlock - 1n));

        const lastWipBucket = writer.wipBucketPath;

        // write first block of next bucket
        writeRange(writer, Number(endBlock), Number(endBlock));
        writer.beginFlush();
        await waitEvent(writer.events, 'flush');

        expect(writer.lastOrdinal).to.be.equal(BigInt(endBlock));

        // expect current wip bucket to change
        expect(writer.wipBucketPath).to.not.be.equal(lastWipBucket);

        // wip table files should exist
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'block.ab.wip'))).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx.ab.wip'))).to.be.true;
        expect(fs.existsSync(path.join(writer.wipBucketPath, 'tx_log.ab.wip'))).to.be.true;
    });

    it('full read & compare', async () => {
        // read all blocks and deep compare, should match
        await compareRange(Number(startBlock), Number(endBlock));
    });
});
