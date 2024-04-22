import crypto from 'node:crypto';

import moment from "moment";

import {ArrowBatchContextDef, RowWithRefs} from "../context.js";
import {ArrowBatchWriter} from "../writer";

export function randomBytes(length: number): Buffer {
    return crypto.randomBytes(length);
}

export function randomHexString(length: number): string {
    return randomBytes(Math.ceil(length / 2)).toString('hex').slice(0, length);
}

export function randomInteger(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

export const sleep = (ms: number) => new Promise(res => setTimeout(res, ms));


// Data generation for mock blockchain

export const testDataContext: ArrowBatchContextDef = {
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


export type TestBlockRow = [bigint, bigint, string, number];

export type TestTxRow = [string, bigint, bigint, number, number, Buffer];

export type TestTxLogRow = [bigint, number, string];

export class TestChainGenerator {

    static readonly BLOCK_TIME_MS = 500;
    static genTxLogRow(
        globalTxIdx: bigint,
        logIndex: number
    ): TestTxLogRow {
        return [
            globalTxIdx,
            logIndex,
            randomHexString(40)
        ];
    }

    static genTxRow(
        globalTxIdx: bigint,
        block: bigint,
        actionOrdinal: number,
        evmOrdinal: number
    ): TestTxRow {
        return [
            randomHexString(64),
            globalTxIdx,
            block,
            actionOrdinal,
            evmOrdinal,
            randomBytes(randomInteger(56, 256))
        ];
    }

    static genBlockRow(
        num: bigint,
        timestamp: bigint,
        txAmountMin: number = 1,
        txAmountMax: number = 5
    ): TestBlockRow {
        return  [
            num,
            timestamp,
            randomHexString(64),
            randomInteger(txAmountMin, txAmountMax)
        ]
    }

    static currentTime(): bigint {
        const utcNow = moment.utc();
        const roundedMS = Math.floor(
            utcNow.milliseconds() / TestChainGenerator.BLOCK_TIME_MS) * TestChainGenerator.BLOCK_TIME_MS;

        return BigInt(utcNow.startOf('second').valueOf() + roundedMS);
    }

    static genTestData(
        startBlock: bigint,
        endBlock: bigint,
        startTimestamp: bigint = undefined
    ): [
        RowWithRefs[],
        (writer: ArrowBatchWriter, from: number, to: number) => void
    ] {
        if (typeof startTimestamp === 'undefined')
            startTimestamp = this.currentTime();

        let globalTxIndex = 0n;
        const blockRows: RowWithRefs[] = [];
        for (let i = startBlock; i <= endBlock; i++) {
            const ts = startTimestamp + ((i - startBlock) * BigInt(TestChainGenerator.BLOCK_TIME_MS));
            const row = this.genBlockRow(i, ts);
            const txAmount = row[3];

            let actionOrdinal = 0;
            const txs: RowWithRefs[] = [];
            for (let evmOrdinal = 0; evmOrdinal < txAmount; evmOrdinal++) {
                const txLogs: RowWithRefs[] = [];
                for (let j = 0; j < randomInteger(1, 5); j++) {
                    txLogs.push({
                        row: this.genTxLogRow(globalTxIndex, j),
                        refs: new Map()
                    });
                }

                const tx = this.genTxRow(
                    globalTxIndex, i, actionOrdinal, evmOrdinal);

                txs.push({
                    row: tx,
                    refs: new Map([['tx_log', txLogs]])
                });

                if (Math.random() < .8)
                    actionOrdinal++;

                globalTxIndex++;
            }
            blockRows.push({row, refs: new Map([['tx', txs]])});
        }

        const writeRange = (writer: ArrowBatchWriter, from: number, to: number) => {
            for (let i = from; i <= to; i++) {
                const block = blockRows[i];
                const txRefs = block.refs.get('tx') ?? [];

                for (const tx of txRefs) {
                    const txLogRefs = tx.refs.get('tx_log') ?? [];

                    for (const txLog of txLogRefs)
                        writer.addRow('tx_log', txLog.row, txLog.row);

                    writer.addRow('tx', tx.row, block.row);
                }
                writer.addRow('block', block.row, block.row);

                writer.updateOrdinal(i);
            }
        };

        return [blockRows, writeRange];
    }
    
}