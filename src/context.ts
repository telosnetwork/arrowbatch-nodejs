import {promises as pfs, readFileSync, existsSync} from "node:fs";
import EventEmitter from "node:events";
import path from "node:path";

import {Logger} from "winston";

import {ArrowTableMapping} from "./protocol.js";
import {ArrowBatchConfig} from "./types.js";


export interface TableBufferInfo {
    columns: Map<
        string,  // column name
        any[]
    >
}

export type RowBuffers = Map<
    string,  // table name
    TableBufferInfo
>;

export interface ArrowBatchContextDef {
    root: {
        name?: string;
        ordinal: string;
        map: ArrowTableMapping[];
    },
    others: {[key: string]: ArrowTableMapping[]}
}

export interface ReferenceMap {
    [key: string]: {  // string key is name of table that references
        parentIndex: number  // column index of parent field
        parentMapping: ArrowTableMapping  // mapping of parent field
        childIndex: number   // column index of reference
        childMapping: ArrowTableMapping  // mapping of child field
    }
}

export interface RowWithRefs {
    row: any[],
    refs: Map<string, RowWithRefs[]>
}

export function objectifyRowWithRefs(row: RowWithRefs): object {
    const refs = {};
    [...row.refs.entries()].forEach(([key, rows]) => {
        refs[key] = rows.map(r => objectifyRowWithRefs(r));
    });
    return {
        row: row.row,
        refs
    }
}

export function generateMappingsFromDefs(definition: ArrowBatchContextDef) {
    const rootOrdField: ArrowTableMapping = {name: definition.root.ordinal, type: 'u64'};
    const rootMap = [rootOrdField, ...definition.root.map];

    const mappings = {
        ...definition.others,
        root: rootMap
    };

    // dont allow more than one ref per table
    for (const [tableName, maps] of Object.entries(mappings)) {
        let refCount = 0;
        for (const field of maps)
            if (field.ref && ++refCount > 1)
                throw new Error(`Multiple references per table not implemented, table ${tableName}, field ${field.name}`);
    }

    return new Map<string, ArrowTableMapping[]>(Object.entries(mappings));
}

export function genereateReferenceMappings(tableName: string, tableMappings: Map<string, ArrowTableMapping[]>): ReferenceMap {
    const refs: ReferenceMap = {};

    const mapping: ArrowTableMapping[] = tableMappings.get(tableName);

    for (const [refName, refMapping] of tableMappings.entries()) {
        const childIndex = refMapping.findIndex(
            m => m.ref && m.ref.table === tableName);

        if (childIndex != -1) {
            const childMapping = refMapping.at(childIndex);
            const parentIndex = mapping.findIndex(
                m => m.name === childMapping.ref.field);
            const parentMapping = mapping.at(parentIndex);

            refs[refName] = {
                parentIndex,
                parentMapping,
                childIndex,
                childMapping
            };
        }

    }

    return refs;
}

export class ArrowBatchContext {
    static readonly DEFAULT_BUCKET_SIZE = BigInt(1e7);
    static readonly DEFAULT_DUMP_SIZE = BigInt(1e5);

    readonly config: ArrowBatchConfig;
    definition: ArrowBatchContextDef;
    readonly logger: Logger;

    events = new EventEmitter();

    // updated by reloadOnDiskBuckets, map adjusted num -> table name -> file name
    tableFileMap: Map<number, Map<string, string>>;
    wipFilesMap: Map<string, string>;

    // setup by parsing context definitions, defines data model
    tableMappings: Map<string, ArrowTableMapping[]>;

    // setup by parsing context definitions, metadata for references in tables
    refMappings = new Map<string, ReferenceMap>();

    protected _firstOrdinal: bigint;
    protected _lastOrdinal: bigint;

    constructor(
        config: ArrowBatchConfig,
        logger: Logger
    ) {
        this.config = config;
        this.logger = logger;

        if (!this.config.writerLogLevel)
            this.config.writerLogLevel = 'INFO';

        if (!this.config.bucketSize)
            this.config.bucketSize = ArrowBatchContext.DEFAULT_BUCKET_SIZE;

        if (!this.config.dumpSize)
            this.config.dumpSize = ArrowBatchContext.DEFAULT_DUMP_SIZE;

        const contextDefsPath = path.join(this.config.dataDir, 'context.json');
        if (existsSync(contextDefsPath)) {
            const definition = JSON.parse(
                readFileSync(contextDefsPath).toString());

            this.definition = definition;
            this.tableMappings = generateMappingsFromDefs(definition);
            for (const tableName of this.tableMappings.keys())
                this.refMappings.set(
                    tableName, genereateReferenceMappings(tableName, this.tableMappings));
        }
    }

    async init(startOrdinal: number | bigint) {
        await pfs.mkdir(this.config.dataDir, {recursive: true});

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

    private async loadTableFileMap(bucket: string) {
        const bucketNum = this.bucketToOrdinal(bucket);
        const bucketFullPath = path.join(this.config.dataDir, bucket);
        const tableFiles = (
            await pfs.readdir(
                path.join(this.config.dataDir, bucket), {withFileTypes: true}))
            .filter(p => p.isFile() && (p.name.endsWith('.ab')) || p.name.endsWith('.ab.wip'))
            .map(p => p.name);

        const tableFilesMap = new Map();
        const wipFilesMap = new Map();
        for (const tableName of this.tableMappings.keys()) {
            let name = tableName;
            if (name === 'root')
                name = this.definition.root.name;

            const wipFile = tableFiles.find(file => file == `${name}.ab.wip`);
            if (wipFile) {
                wipFilesMap.set(
                    tableName,
                    path.join(bucketFullPath, wipFile)
                );
            }

            const file = tableFiles.find(file => file == `${name}.ab`);
            if (file) {
                tableFilesMap.set(
                    tableName,
                    path.join(bucketFullPath, file)
                );
            }
        }
        this.tableFileMap.set(bucketNum, tableFilesMap);

        // if wip files are found in this bucket
        if (wipFilesMap.size > 0) {
            // ensure this is the only bucket wip files are found
            if (this.wipFilesMap.size > 0)
                throw new Error(
                    `Found ${wipFilesMap.size} wip files in bucket ` +
                    `${bucketNum} but had already found wip files in prev bucket!`
                );

            this.wipFilesMap = wipFilesMap;
        }
    }

    async reloadOnDiskBuckets() {
        this.tableFileMap = new Map();
        this.wipFilesMap = new Map();
        const sortNameFn = (a: string, b: string) => {
            const aNum = this.bucketToOrdinal(a);
            const bNum = this.bucketToOrdinal(b);
            if (aNum < bNum)
                return -1;
            if (aNum > bNum)
                return 1;
            return 0;
        };

        const bucketDirs = (
            await pfs.readdir(
                this.config.dataDir, {withFileTypes: true}))
            .filter(p => p.isDirectory())
            .map(p => p.name)
            .sort(sortNameFn);

        await Promise.all(
            bucketDirs.map(bucket => this.loadTableFileMap(bucket)));
    }
}