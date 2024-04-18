import path from "node:path";
import fs from "node:fs";
import os from "node:os";

import { Logger } from 'winston';

import {
    ArrowBatchCompression,
    ArrowBatchConfig,
    ArrowBatchContextDef,
    ArrowBatchWriter,
    createLogger
} from '../index.js';
import {randomBytes, randomHexString, randomInteger, waitEvent} from "./utils.js";

describe('ArrowBatchWriter', () => {


    let config: ArrowBatchConfig;

    before((done) => {
        // Create a unique temporary directory
        const tmpDirPrefix = path.join(os.tmpdir(), 'test-writer');
        fs.mkdtemp(tmpDirPrefix, (err, tmpDir) => {
            if (err) return done(err);
            config = {
                dataDir: tmpDir,
                compression: ArrowBatchCompression.ZSTD,
                writerLogLevel: 'info',
                bucketSize: 100n,
                dumpSize: 10n,
            };
            done();
        });
    });

    after((done) => {
        // Clean up the temporary directory after all tests are done
        fs.rmdir(config.dataDir, { recursive: true }, (err) => {
            if (err) return done(err);
            done();
        });
    });

    const logger = createLogger('testWriter', 'info');

    const testDataContext: ArrowBatchContextDef = {
        root: {
            name: 'block',
            ordinal: 'block_num',
            map: [
                { name: 'timestamp', type: 'u64' },
                { name: 'block_hash', type: 'checksum256' },
                { name: 'txs_amount', type: 'u32' },
            ],
        },
        others: {
            tx: [
                { name: 'id', type: 'checksum256' },
                { name: 'global_index', type: 'u64' },
                { name: 'block_num', type: 'u64', ref: { table: 'root', field: 'block_num' } },
                { name: 'action_ordinal', type: 'u32' },
                { name: 'evm_ordinal', type: 'u32' },
                { name: 'raw', type: 'bytes' },
            ],
            tx_log: [
                { name: 'tx_index', type: 'u64', ref: { table: 'tx', field: 'global_index' } },
                { name: 'log_index', type: 'u32' },
                { name: 'address', type: 'checksum160', optional: true },
            ],
        },
    };

    let globalIndex = 0n;

    const genRandomLog = (
        globalIndex: bigint,
        logIndex: number
    ) => {
        return [
            globalIndex,
            logIndex,
            randomHexString(40)
        ];
    }

    const genRandomTx = (
        block: bigint,
        actionOrdinal: number, evmOrdinal: number
    ) => {
        return [
            randomHexString(64),
            globalIndex++,
            block,
            actionOrdinal,
            evmOrdinal,
            randomBytes(randomInteger(56, 256))
        ];
    }

    const genTestData = (startBlock: bigint, endBlock: bigint, startTimestamp: bigint) => {
        const blocks = [];
        const txs = [];
        const txLogs = [];
        for (let i = startBlock; i < endBlock; i++) {
            const ts = startTimestamp + ((i - startBlock) * 500n);
            const txAmount = randomInteger(0, 10);
            const blockRow = [
                i,
                ts,
                randomHexString(64),
                txAmount
            ];

            let actionOrdinal = 0;
            for (let evmOrdinal = 0; evmOrdinal < txAmount; evmOrdinal++) {
                for (let j = 0; j < randomInteger(0, 5); j++)
                    txLogs.push(genRandomLog(globalIndex, j));

                const tx = genRandomTx(
                    i, actionOrdinal, evmOrdinal
                );

                txs.push(tx);

                if (Math.random() < .8)
                    actionOrdinal++;
            }
            blocks.push(blockRow);
        }
        return {blocks, txs, txLogs};
    };

    const startBlock = 1n;
    const endBlock = 1000n;
    const testData = genTestData(startBlock, endBlock, 0n);

    const txsForBlock = (blockNum: bigint) => {
        const txs = [];
        for (const tx of testData.txs)
            if (tx[2] == blockNum)
                txs.push(tx);
            else if (txs.length > 0)
                break;
        return txs;
    };

    const logsForTx = (globalIndex: bigint) => {
        const logs = [];
        for (const log of testData.txLogs)
            if (log[0] == globalIndex)
                logs.push(log);
            else if (logs.length > 0)
                break;
        return logs;
    };

    let writer: ArrowBatchWriter;

    beforeEach(async () => {
        writer = new ArrowBatchWriter(config, testDataContext, logger);
        await writer.init(startBlock);
    });

    afterEach(async () => {
        await writer.deinit();
    });

    it('write first 10', async () => {
        for (let i = 0; i < 10; i++) {
            const blockRow = testData.blocks[i];
            const txs = txsForBlock(blockRow[0]);
            const txLogs = [];
            txs.forEach(tx => txLogs.push(logsForTx(tx[1])));

            writer.addRow('block', testData.blocks[i]);
            for (const tx of txs)
                writer.addRow('tx', tx, blockRow);

            for (const txLog of txLogs)
                writer.addRow('tx_log', txLog, blockRow);
        }

        writer.beginFlush();

        await waitEvent(writer.events, 'flush');
    });
});
