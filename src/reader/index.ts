import {Logger} from "winston";

import {ArrowBatchFileMetadata, ArrowBatchProtocol, decodeRowValue, DEFAULT_ALIAS} from "../protocol.js";
import {ArrowBatchConfig, FlushReq} from "../types.js";
import {
    ArrowBatchContext, ArrowBatchContextDef
} from "../context.js";
import {ArrowBatchCache} from "../cache.js";

import {ArrowBatchBroadcastClient} from "./broadcast.js";
import {sleep} from "../utils.js";

export interface TableBuffers {
    columns: Map<string, any[]>;
}


export class ArrowBatchReader extends ArrowBatchContext {

    // intermediate holds the current table we are building
    protected _intermediateBuffers: TableBuffers;

    // on flush operations, data from intermediate gets pushed to auxiliary and intermediate is reset
    // auxiliary will get cleared as flush operations finish
    protected _auxiliaryBuffers: TableBuffers;

    protected cache: ArrowBatchCache;

    wsClient: ArrowBatchBroadcastClient;

    private syncTaskRunning = false;

    constructor(
        config: ArrowBatchConfig,
        definition: ArrowBatchContextDef | undefined,
        logger: Logger
    ) {
        super(config, logger);

        if (!this.definition) {
            if (!definition)
                throw new Error('Could not figure out data context from disk, and no default provided!');

            this.setDataDefs(definition);
        }

        this.cache = new ArrowBatchCache(this);

        this._intermediateBuffers = this._createBuffer();
        this._initIntermediate();
    }

    get firstOrdinal(): bigint {
        return this._firstOrdinal;
    }

    get lastOrdinal(): bigint {
        return this._lastOrdinal;
    }

    protected _createBuffer(): TableBuffers {
        const buffers = {columns: new Map<string, any[]>()};
        for (const mapping of this.tableMapping)
            buffers.columns.set(mapping.name, []);
        return buffers;
    }

    protected _initIntermediate() {
        this._intermediateBuffers = this._createBuffer();
    }

    pushRawRow(row: any[]) {
        for (const [i, mapping] of this.tableMapping.entries())
            this._intermediateBuffers.columns.get(mapping.name).push(decodeRowValue(mapping, row[i]));
    }

    pushRow(row: any[]) {
        for (const [i, mapping] of this.tableMapping.entries())
            this._intermediateBuffers.columns.get(mapping.name).push(row[i]);
    }

    async init() {
        // context init will reload disks
        await super.init();

        // wip files found, load unfinished table into buffers and init partially
        if (this.wipFile) {
            const fileMeta = await ArrowBatchProtocol.readFileMetadata(this.wipFile);

            if (fileMeta.batches.length > 1)
                throw new Error(`Expected on-disk wip table to have only one batch!`);

            const metadata = fileMeta.batches[0];

            const wipTable = await ArrowBatchProtocol.readArrowBatchTable(this.wipFile, fileMeta, 0);

            // maybe load lastOrdinal from there
            if (wipTable.numRows > 0) {
                const lastRow = wipTable.get(wipTable.numRows - 1).toArray();
                this._lastOrdinal = lastRow[this.ordinalIndex];

                // sanity check
                if (this._lastOrdinal !== metadata.batch.lastOrdinal)
                    throw new Error(
                        `Mismatch between table lastOrdinal (${this._lastOrdinal.toLocaleString()}) and metadata\'s (${metadata.batch.lastOrdinal.toLocaleString()})`)
            }

            // load all rows into ram buffers
            for (let i = 0; i < wipTable.numRows; i++)
                this.pushRawRow(wipTable.get(i).toArray());
        }

        // load initial state from disk tables
        if (this.tableFileMap.size > 0) {
            const lowestBucket = [...this.tableFileMap.keys()]
                .sort((a, b) => a - b)[0];

            const highestBucket = [...this.tableFileMap.keys()]
                .sort((a, b) => b - a)[0];

            const rootFirstPath = this.tableFileMap.get(lowestBucket);
            const firstMetadata = (await ArrowBatchProtocol.readFileMetadata(rootFirstPath)).batches[0];
            const firstTableSize = firstMetadata.batch.lastOrdinal - firstMetadata.batch.startOrdinal;
            if (firstTableSize > 0n)
                this._firstOrdinal = firstMetadata.batch.startOrdinal;

            // only load if lastOrdinal isn't set, (will be set if loaded wip)
            if (typeof this._lastOrdinal === 'undefined') {
                const rootLastPath = this.tableFileMap.get(highestBucket);
                const lastFileMetadata = await ArrowBatchProtocol.readFileMetadata(rootLastPath);
                const lastMetadata = lastFileMetadata.batches[lastFileMetadata.batches.length - 1];
                const lastTableSize = lastMetadata.batch.lastOrdinal - lastMetadata.batch.startOrdinal;

                if (lastTableSize > 0)
                    this._lastOrdinal = lastMetadata.batch.lastOrdinal;
            }
        }

        this.logger.debug(`on disk info: ${this._firstOrdinal} to ${this._lastOrdinal}`);
    }

    async beginSync(
        connectTimeout: number = 5000,
        callbacks: {
            onRow?: (row: any[]) => void,
            onFlush?: (info: FlushReq['params']) => void
        } = {}
    ) {
        const url = `ws://${this.config.wsHost}:${this.config.wsPort}`;
        let attempt = 1;
        if (!this.wsClient || !this.wsClient.isConnected()) {
            // attempt connection to live feed
            this.wsClient = new ArrowBatchBroadcastClient({
                url,
                logger: this.logger,
                ordinalIndex: this.ordinalIndex,
                handlers: {
                    pushRow: (row: any[]) => {
                        this.pushRow(row);
                        if (callbacks.onRow)
                            callbacks.onRow(row);
                    },
                    flush: (info: FlushReq['params']) => {
                        this.reloadOnDiskBuckets().then(() => {
                            this._initIntermediate();
                            if (callbacks.onFlush)
                                callbacks.onFlush(info);
                        });
                    }
                }
            });

            this.wsClient.connect();

            const startConnectTime = performance.now();
            while (!this.wsClient.isConnected()) {
                if (performance.now() - startConnectTime > connectTimeout)
                    break;
                attempt++;
                await sleep(100);
            }
        }

        if (!this.wsClient.isConnected())
            throw new Error(`Tried to connect to ${url}, after ${attempt} attempts`)

        const liveInfo = await this.wsClient.getInfo();
        const serverLastOrdinal = BigInt(liveInfo.result.last_ordinal);
        this.logger.debug(`server last ordinal: ${serverLastOrdinal.toLocaleString()}`);

        let from: bigint;
        // if _lastOrdinal is defined means we know about some on disk buckets,
        // sync from last known block forward
        if (this._lastOrdinal) {
            const liveDelta = serverLastOrdinal - this._lastOrdinal;
            this.logger.debug(`missing ${liveDelta} rows. need to fetch from socket`);
            from = this._lastOrdinal + 1n;
        } else
            from = serverLastOrdinal;

        await this.wsClient.sync({from: from.toString()});
    }

    protected getColumn(columnName: string, auxiliary: boolean = false) {
        const tableBuffers = auxiliary ? this._auxiliaryBuffers : this._intermediateBuffers;
        return tableBuffers.columns.get(columnName);
    }

    get auxiliarySize(): number {
        return this.getColumn(this.definition.ordinal, true).length;
    }

    get auxiliaryLastOrdinal(): bigint {
        return this.getColumn(this.definition.ordinal, true)[this.auxiliarySize - 1];
    }

    get intermediateSize(): number {
        return this.getColumn(this.definition.ordinal).length;
    }

    get intermediateFirstOrdinal(): bigint {
        return this.getColumn(this.definition.ordinal)[0];
    }

    get intermediateLastOrdinal(): bigint {
        return this.getColumn(this.definition.ordinal)[this.intermediateSize - 1];
    }

    get cacheSize(): number {
        return this.cache.size;
    }

    protected getBufferRow(index: number, auxiliary: boolean = false): any[] {
        const tableBuffers = auxiliary ? this._auxiliaryBuffers : this._intermediateBuffers;
        return this.tableMapping.map(
            m => tableBuffers.columns.get(m.name)[index]);
    }

    static getRelativeTableIndex(ordinal: bigint, metadata: ArrowBatchFileMetadata): [number, bigint] {
        // ensure bucket contains ordinal
        const bucketOrdStart = metadata.batches[0].batch.startOrdinal;
        const bucketOrdLast = metadata.batches[
            metadata.batches.length - 1].batch.lastOrdinal;

        if (ordinal < bucketOrdStart || ordinal > bucketOrdLast)
            throw new Error(`Ordinal ${ordinal} is not in bucket range (${bucketOrdStart}-${bucketOrdLast}).`);

        let batchIndex = 0;
        while (ordinal > metadata.batches[batchIndex].batch.lastOrdinal) {
            batchIndex++;
        }

        return [batchIndex, ordinal - metadata.batches[batchIndex].batch.startOrdinal];
    }

    async getRow(ordinal: bigint): Promise<any[]> {
        const ordinalField = this.definition.ordinal;

        const maybeGetRowFromBuffers = (buffers: TableBuffers): any[] | null => {
            if (buffers && buffers.columns.get(ordinalField).length > 0) {
                const ordinalField = this.tableMapping[this.ordinalIndex];
                const ordinalColumn = buffers.columns.get(ordinalField.name);
                const oldestOnBuffer = ordinalColumn[0];
                const newestOnBuffer = ordinalColumn[ordinalColumn.length - 1];
                const isOnBuffer = ordinal >= oldestOnBuffer && ordinal <= newestOnBuffer;
                if (isOnBuffer) {
                    const index = Number(ordinal - oldestOnBuffer);
                    return this.getBufferRow(index);
                }
            }
            return null;
        };

        let maybeRow: any[] | null;

        // is row in intermediate buffers?
        maybeRow = maybeGetRowFromBuffers(this._intermediateBuffers);
        if (maybeRow)
            return maybeRow;

        // is row in auxiliary buffers?
        maybeRow = maybeGetRowFromBuffers(this._auxiliaryBuffers);
        if (maybeRow)
            return maybeRow;

        // is row on disk?
        const [table, batchIndex] = await this.cache.getTableFor(ordinal);

        // fetch requested row from root table
        const adjustedOrdinal = this.getOrdinal(ordinal);
        const [bucketMetadata, _] = await this.cache.getMetadataFor(adjustedOrdinal);
        const [__, relativeIndex] = ArrowBatchReader.getRelativeTableIndex(ordinal, bucketMetadata);
        const structRow = table.get(Number(relativeIndex));

        if (!structRow)
            throw new Error(`Could not find row ${ordinal}! ao: ${adjustedOrdinal} bi: ${batchIndex} ri: ${relativeIndex}`);

        const row = structRow.toArray();
        this.tableMapping.forEach((m, i) => {
            row[i] = decodeRowValue(m, row[i]);
        });

        return row;
    }

    async validate() {
        for (const adjustedOrdinal of [...this.tableFileMap.keys()].sort()) {
            const [bucketMeta, _] = await this.cache.getMetadataFor(adjustedOrdinal);

            for (const [batchIndex, batchMeta] of bucketMeta.batches.entries()) {
                this.logger.info(`validating bucket ${adjustedOrdinal} batch ${batchIndex + 1}/${bucketMeta.batches.length}`);

                const metaSize = Number(batchMeta.batch.lastOrdinal - batchMeta.batch.startOrdinal) + 1;

                const [_, table] = await this.cache.directLoadTable(adjustedOrdinal, batchIndex);
                const tableSize = table.numRows;
                const actualStart = table.get(0).toArray()[0] as bigint;
                const actualLast = table.get(tableSize - 1).toArray()[0] as bigint;

                if (metaSize !== tableSize) {
                    this.logger.error(`metaSize (${metaSize}) != tableSize (${tableSize})`);

                    let lastEval = actualStart - 1n;
                    for (const [i, row] of table.toArray().entries()) {
                        const ord = row.toArray()[0] as bigint;
                        if (ord !== lastEval + 1n)
                            throw new Error(
                                `table row size metadata mismatch at table index ${i} expected ${lastEval + 1n} and got ${ord}`);

                        lastEval = ord;
                    }
                }
//                    throw new Error(`table row size metadata mismatch!`);

                if (batchMeta.batch.startOrdinal !== actualStart)
                    throw new Error(`batch metadata startOrd mismatch with actual!`);

                if (batchMeta.batch.lastOrdinal !== actualLast)
                    throw new Error(`batch metadata lastOrd mismatch with actual!`);
            }
        }
    }
}