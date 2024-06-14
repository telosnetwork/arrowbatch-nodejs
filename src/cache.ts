import {Table} from "apache-arrow";

import {ArrowBatchFileMetadata, ArrowBatchProtocol} from "./protocol.js";
import {ArrowBatchContext} from "./context.js";
import {promises as fs} from "node:fs";
import {ArrowBatchReader} from "./reader/index.js";
import {LRUCache} from "typescript-lru-cache";


export class ArrowBatchCache {
    static readonly DEFAULT_CACHE_SIZE = 10;

    private ctx: ArrowBatchContext;

    // actual arrow tables, key: ${bucket}-${batchIndex}
    private tableCache = new LRUCache<string, Table>(
        {maxSize: ArrowBatchCache.DEFAULT_CACHE_SIZE}
    );

    // bucket metadata, key: bucket
    private metadataCache = new LRUCache<number, ArrowBatchFileMetadata>(
        {maxSize: ArrowBatchCache.DEFAULT_CACHE_SIZE}
    );

    readonly dataDir: string;

    constructor(ctx: ArrowBatchContext) {
        this.ctx = ctx;
        this.ctx.logger.debug(`intialized new cache`);
    }

    async getMetadataFor(adjustedOrdinal: number): Promise<[ArrowBatchFileMetadata, boolean]> {
        this.ctx.logger.debug(`get meta for ${adjustedOrdinal}`);
        const filePath = this.ctx.tableFileMap.get(adjustedOrdinal);
        const bucketStat = await fs.stat(filePath);

        if (this.metadataCache.has(adjustedOrdinal)) {
            this.ctx.logger.debug(`cache already has key ${adjustedOrdinal}`);
            // we might need to invalidate our metadata if file size changed
            const cachedMeta  = this.metadataCache.get(adjustedOrdinal);
            if (cachedMeta.size === bucketStat.size) {
                this.ctx.logger.debug(`cached meta size matches file! skip reload ${cachedMeta.size}`);
                return [cachedMeta, false];  // size hasn't changed, return cache
            }
        }

        // reload
        const meta = await ArrowBatchProtocol.readFileMetadata(filePath);
        this.metadataCache.set(adjustedOrdinal, meta);
        this.ctx.logger.debug(`meta cache updated for ${adjustedOrdinal}`);
        return [meta, true];
    }

    async directLoadTable(
        adjustedOrdinal: number,
        batchIndex: number,
        metadata?: ArrowBatchFileMetadata
    ): Promise<Table> {
        const filePath = this.ctx.tableFileMap.get(adjustedOrdinal);

        if (typeof filePath === 'undefined')
            throw new Error(`No known table for ${adjustedOrdinal}, maybe need to reload on disk buckets...`);

        if (!metadata)
            metadata = await ArrowBatchProtocol.readFileMetadata(filePath);

        if (batchIndex >= metadata.batches.length)
            throw new Error(`Requested batch index is > than available according to metadata...`);

        const table = await ArrowBatchProtocol.readArrowBatchTable(
            filePath, metadata, batchIndex);

        this.ctx.logger.debug(`direct load bucket ${adjustedOrdinal} table ${batchIndex}`);

        return table;
    }

    async getTableFor(ordinal: bigint): Promise<[Table, number]> {
        this.ctx.logger.debug(`cache: get table for ${ordinal}`)
        const adjustedOrdinal = this.ctx.getOrdinal(ordinal);

        // metadata about the bucket we are going to get tables for, mostly need to
        // figure out start ordinal for math to make sense in case non-aligned bucket
        // boundary start
        const [bucketMetadata, metadataUpdated] = await this.getMetadataFor(adjustedOrdinal);
        const [batchIndex, _] = ArrowBatchReader.getRelativeTableIndex(ordinal, bucketMetadata);

        const cacheKey = `${adjustedOrdinal}-${batchIndex}`;
        if (this.tableCache.has(cacheKey)) {
            // we have this tables cached, but only return if metadata wasn't invalidated
            if (!metadataUpdated)
                return [this.tableCache.get(cacheKey), batchIndex];

        }

        // load table from disk file
        const table = await this.directLoadTable(adjustedOrdinal, batchIndex, bucketMetadata);
        this.tableCache.set(cacheKey, table);

        return [table, batchIndex];
    }

    get size() : number {
        return this.tableCache.size;
    }
}