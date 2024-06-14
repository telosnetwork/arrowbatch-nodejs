import crypto from 'node:crypto';

import moment from "moment";

import {ArrowBatchContextDef} from "../context.js";
import {ArrowBatchWriter} from "../writer";
import {expect} from "chai";

export function randomBytes(length: number): Buffer {
    return crypto.randomBytes(length);
}

export function randomHexString(length: number): string {
    return randomBytes(Math.ceil(length / 2)).toString('hex').slice(0, length);
}

export function randomInteger(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}


// Data generation for mock blockchain

export const testDataContext: ArrowBatchContextDef = {
    alias: 'blocks',
    ordinal: 'block_num',
    map: [
        { name: 'block_num', type: "u64"},
        { name: 'timestamp', type: 'u64' },
        { name: 'block_hash', type: 'checksum256' },
        { name: 'txs', type: 'bytes' },
        { name: 'txs_amount', type: 'u32' },
    ],
};


export type TestBlockRow = [
    bigint,
    bigint,
    Buffer | string,
    Buffer | string,
    number
];


export class TestChainGenerator {

    static readonly BLOCK_TIME_MS = 500;

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
            randomBytes(512),
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
        TestBlockRow[],
        (writer: ArrowBatchWriter, from: number, to: number) => void
    ] {
        if (typeof startTimestamp === 'undefined')
            startTimestamp = this.currentTime();

        const blockRows  = [];
        for (let i = startBlock; i <= endBlock; i++) {
            const ts = startTimestamp + ((i - startBlock) * BigInt(TestChainGenerator.BLOCK_TIME_MS));
            const row = this.genBlockRow(i, ts);
            blockRows.push(row);
        }

        const writeRange = (writer: ArrowBatchWriter, from: number, to: number) => {
            if (writer.lastOrdinal)
                expect(writer.lastOrdinal).to.be.eq(BigInt(from) - 1n);
            for (let i = from; i <= to; i++) {
                const block = blockRows[i];
                writer.pushRow(block);
            }
        };

        return [blockRows, writeRange];
    }
    
}