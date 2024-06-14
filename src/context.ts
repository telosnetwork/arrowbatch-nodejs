import {promises as pfs, readFileSync, existsSync} from "node:fs";
import EventEmitter from "node:events";
import path from "node:path";

import {Logger} from "winston";

import {ArrowTableMapping, DEFAULT_ALIAS} from "./protocol.js";
import {ArrowBatchConfig} from "./types.js";


export interface ArrowBatchContextDef {
    ordinal: string;
    map: ArrowTableMapping[];
    alias?: string;
    stream_size?: number | string;
}

export class ArrowBatchContext {

    readonly config: ArrowBatchConfig;
    definition: ArrowBatchContextDef;
    readonly logger: Logger;
    alias: string = DEFAULT_ALIAS;
    ordinalIndex: number = 0;

    events: EventEmitter = new EventEmitter();

    // updated by reloadOnDiskBuckets
    tableFileMap = new Map<number, string>();

    // setup by parsing context definitions, defines data model
    tableMapping: ArrowTableMapping[];
    protected wipFile: string;

    protected _firstOrdinal: bigint;
    protected _lastOrdinal: bigint;

    constructor(
        config: ArrowBatchConfig,
        logger: Logger
    ) {
        this.config = config;
        this.logger = logger;

        const contextDefsPath = path.join(this.config.dataDir, 'context.json');
        if (existsSync(contextDefsPath)) {
            this.setDataDefs(JSON.parse(
                readFileSync(contextDefsPath).toString()));
        }
    }

    setDataDefs(defs: ArrowBatchContextDef) {
        this.alias = defs.alias ?? DEFAULT_ALIAS;
        this.ordinalIndex = defs.map.findIndex(mapping => mapping.name === defs.ordinal);
        this.tableMapping = defs.map;
        this.definition = defs;
    }

    async init() {
        await this.reloadOnDiskBuckets();
    }

    getOrdinal(ordinal: number | bigint): number {
        ordinal = BigInt(ordinal);
        return Number(ordinal / BigInt(this.config.bucketSize));
    }

    getOrdinalSuffix(ordinal: number | bigint): string {
        return String(this.getOrdinal(ordinal)).padStart(8, '0');
    }

    private bucketToOrdinal(tableBucketName: string): number {
        if (tableBucketName.includes('.wip'))
            tableBucketName = tableBucketName.replace('.wip', '');

        const match = tableBucketName.match(/\d+/);
        return match ? parseInt(match[0], 10) : NaN;
    }

    async reloadOnDiskBuckets() {
        this.logger.debug(`table map length before reload: ${this.tableFileMap.size}`);

        this.tableFileMap = new Map();
        this.wipFile = undefined;
        const sortNameFn = (a: string, b: string) => {
            const aNum = this.bucketToOrdinal(a);
            const bNum = this.bucketToOrdinal(b);
            if (aNum < bNum)
                return -1;
            if (aNum > bNum)
                return 1;

            throw new Error(`Error sorting bucket dirs, found duplicates!`);
        };

        const bucketDirs = (
            await pfs.readdir(
                this.config.dataDir, {withFileTypes: true}))
            .filter(p => p.isDirectory())
            .map(p => p.name)
            .sort(sortNameFn);

        const loadBucket = async (bucketDir: string) => {
            const bucketNum = this.bucketToOrdinal(bucketDir);
            const bucketFullPath = path.join(this.config.dataDir, bucketDir);
            const tableFiles = (
                await pfs.readdir(
                    path.join(this.config.dataDir, bucketDir), {withFileTypes: true}))
                .filter(p => p.isFile() && p.name.startsWith(this.alias))
                .map(p => p.name);

            const _wipFile = tableFiles.find(file => file.endsWith('.wip'));
            if (_wipFile)
                this.wipFile = path.join(bucketFullPath, _wipFile);

            const file = tableFiles.find(file => file.endsWith('.ab'));
            if (file)
                this.tableFileMap.set(bucketNum, path.join(bucketFullPath, file));
        };

        await Promise.all(bucketDirs.map(dir => loadBucket(dir)));

        this.logger.debug(`table map length after reload: ${this.tableFileMap.size}`);
    }
}