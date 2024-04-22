import {Table} from "apache-arrow";

import {ArrowBatchFileMetadata, ArrowBatchProtocol} from "./protocol.js";
import {ArrowBatchContext} from "./context.js";
import moment from "moment";


export interface ArrowMetaCacheEntry {
    ts: number,
    meta: ArrowBatchFileMetadata, startRow: any[]
}

export interface ArrowCachedTables {
    root: Table,
    others: {[key: string]: Table}
}

export function isCachedTables(obj: any): obj is ArrowCachedTables {
    return (typeof obj === 'object' &&
        typeof obj.root === 'object' &&
        obj.root instanceof Table &&
        typeof obj.others === 'object' &&
        Object.values(obj.others).every((table) => table instanceof Table));
}

export class ArrowBatchCache {
    static readonly DEFAULT_TABLE_CACHE = 10;

    private ctx: ArrowBatchContext;

    private tableCache = new Map<string, ArrowCachedTables>();
    private cacheOrder: string[] = [];

    private metadataCache = new Map<string, ArrowMetaCacheEntry>();

    readonly dataDir: string;

    constructor(ctx: ArrowBatchContext) {
        this.ctx = ctx;
    }

    async getMetadataFor(adjustedOrdinal: number, tableName: string): Promise<[ArrowMetaCacheEntry, boolean]> {
        const filePath = this.ctx.tableFileMap.get(adjustedOrdinal).get(tableName);
        const meta = await ArrowBatchProtocol.readFileMetadata(filePath);

        const cacheKey = `${adjustedOrdinal}-${tableName}`

        if (this.metadataCache.has(cacheKey)) {
            // we might need to invalidate our metadata if file size changed
            const cachedMeta  = this.metadataCache.get(cacheKey);
            if (cachedMeta.meta.size === meta.size)
                return [cachedMeta, false];  // size hasnt change, return cache

            // invalidate and re-calculate
            this.metadataCache.delete(cacheKey);
        }

        const firstTable = await ArrowBatchProtocol.readArrowBatchTable(
            filePath, meta, 0);

        const startRow = firstTable.get(0).toArray();

        const result = { ts: moment.now(), meta, startRow };
        this.metadataCache.set(cacheKey, result);
        return [result, true];
    }

    private async directLoadTable(
        tableName: string,
        adjustedOrdinal: number,
        batchIndex: number
    ): Promise<[string, Table | null]> {
        const filePath = this.ctx.tableFileMap.get(adjustedOrdinal).get(tableName);

        if (typeof filePath === 'undefined')
            return [tableName, null];

        const metadata = await ArrowBatchProtocol.readFileMetadata(filePath);
        return [
            tableName,
            await ArrowBatchProtocol.readArrowBatchTable(
                filePath, metadata, batchIndex)
        ];
    }

    async getTablesFor(ordinal: bigint) {
        const adjustedOrdinal = this.ctx.getOrdinal(ordinal);

        // metadata about the bucket we are going to get tables for, mostly need to
        // figure out start ordinal for math to make sense in case non-aligned bucket
        // boundary start
        const [bucketMetadata, metadataUpdated] = await this.getMetadataFor(adjustedOrdinal, 'root');

        // index relative to bucket boundary
        const relativeIndex = ordinal - bucketMetadata.startRow[0];

        // get batch table index, assuming config.dumpSize table size is respected
        const batchIndex = Number(relativeIndex / BigInt(this.ctx.config.dumpSize));

        const cacheKey = `${adjustedOrdinal}-${batchIndex}`;

        if (this.tableCache.has(cacheKey)) {
            // we have this tables cached, but only return if metadata wasnt invalidated
            if (!metadataUpdated)
                return this.tableCache.get(cacheKey);

            // delete stale cache
            this.tableCache.delete(cacheKey)
        }

        // load all tables from disk files in parallel
        const tableLoadList = await Promise.all([
            this.directLoadTable('root', adjustedOrdinal, batchIndex),
            ...[...this.ctx.tableMappings.keys()].map(
                tableName => this.directLoadTable(tableName, adjustedOrdinal, batchIndex))
        ]);

        const tables: ArrowCachedTables = {
            root: tableLoadList.shift()[1],
            others: {}
        }

        for (const [tableName, table] of tableLoadList)
            if (table instanceof Table)
                tables.others[tableName] = table;

        this.tableCache.set(cacheKey, tables);
        this.cacheOrder.push(cacheKey);

        // maybe trim cache
        if (this.tableCache.size > ArrowBatchCache.DEFAULT_TABLE_CACHE) {
            const oldest = this.cacheOrder.shift();
            this.tableCache.delete(oldest);
        }

        return tables;
    }

    get size() : number {
        return this.tableCache.size;
    }
}