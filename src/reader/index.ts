import {Logger} from "winston";

import {ArrowBatchProtocol, ArrowTableMapping, decodeRowValue} from "../protocol.js";
import {ArrowBatchConfig} from "../types.js";
import {
    ArrowBatchContext, ArrowBatchContextDef, generateMappingsFromDefs, genereateReferenceMappings,
    RowBuffers,
    RowWithRefs,
    TableBufferInfo
} from "../context.js";
import {ArrowBatchCache, ArrowCachedTables, isCachedTables} from "../cache.js";


export class ArrowBatchReader extends ArrowBatchContext {

    // intermediate holds the current table we are building
    protected _intermediateBuffers: RowBuffers = new Map<string, TableBufferInfo>();

    // on flush operations, data from intermediate gets pushed to auxiliary and intermediate is reset
    // auxiliary will get cleared as flush operations finish
    protected _auxiliaryBuffers: RowBuffers = new Map<string, TableBufferInfo>();

    private isFirstUpdate: boolean = true;
    protected cache: ArrowBatchCache;

    constructor(
        config: ArrowBatchConfig,
        definition: ArrowBatchContextDef,
        logger: Logger
    ) {
        super(config, logger);

        if (!this.definition) {
            this.definition = definition;
            this.tableMappings = generateMappingsFromDefs(definition);
            for (const tableName of this.tableMappings.keys())
                this.refMappings.set(
                    tableName, genereateReferenceMappings(tableName, this.tableMappings));
        }

        this.cache = new ArrowBatchCache(this);

        this._intermediateBuffers = this._createBuffers();
        this._initIntermediate();
    }

    get firstOrdinal(): bigint {
        return this._firstOrdinal;
    }

    get lastOrdinal(): bigint {
        return this._lastOrdinal;
    }

    protected _createBuffer(tableName: string) {
        const buffers = {columns: new Map<string, any[]>()};
        for (const mapping of this.tableMappings.get(tableName))
            buffers.columns.set(mapping.name, []);
        return buffers;
    }

    protected _createBuffers() {
        const buffers = new Map<string, TableBufferInfo>();
        for (const tableName of this.tableMappings.keys())
            buffers.set(tableName, this._createBuffer(tableName));
        return buffers;
    }

    protected _initIntermediate() {
        this._auxiliaryBuffers = this._intermediateBuffers;
        this._intermediateBuffers = this._createBuffers();
        this.logger.debug(`initialized buffers for ${[...this._intermediateBuffers.keys()]}`);
    }

    protected _pushRawRow(tableName: string, row: any[]) {
        if (tableName === this.definition.root.name)
            tableName = 'root';

        const tableBuffers = this._intermediateBuffers.get(tableName);
        const mappings = this.tableMappings.get(tableName);
        for (const [i, mapping] of mappings.entries())
            tableBuffers.columns.get(mapping.name).push(decodeRowValue(tableName, mapping, row[i]));
    }

    async init(startOrdinal: number | bigint) {
        await super.init(startOrdinal);

        // wip files found, load unfinished table into buffers and init partially
        if (this.wipFilesMap.size > 0) {
            for (const [tableName, tablePath] of this.wipFilesMap.entries()) {
                const metadata = await ArrowBatchProtocol.readFileMetadata(tablePath);

                if (metadata.batches.length > 1)
                    throw new Error(`Expected on-disk wip table to have only one batch!`);

                const wipTable = await ArrowBatchProtocol.readArrowBatchTable(
                    tablePath, metadata, 0);

                // if its root load lastOrdinal from there
                if (tableName === 'root' && wipTable.numRows > 0) {
                    const lastRow = wipTable.get(wipTable.numRows - 1).toArray();
                    this._lastOrdinal = lastRow[0];
                }

                // load all rows using helper
                for (let i = 0; i < wipTable.numRows; i++)
                    this._pushRawRow(tableName, wipTable.get(i).toArray());
            }
        }

        // load initial state from disk tables
        if (this.tableFileMap.size > 0) {
            const lowestBucket = [...this.tableFileMap.keys()]
                .sort()[0];

            const highestBucket = [...this.tableFileMap.keys()]
                .sort()
                .reverse()[0];

            const rootFirstPath = this.tableFileMap.get(lowestBucket).get('root');
            const firstMetadata = await ArrowBatchProtocol.readFileMetadata(rootFirstPath);
            const firstTable = await ArrowBatchProtocol.readArrowBatchTable(
                rootFirstPath, firstMetadata, 0
            );
            if (firstTable.numRows > 0) {
                const firstRow = firstTable.get(0).toArray();
                this._firstOrdinal = firstRow[0];
            }

            // only load if lastOrdinal isnt set, (will be set if loaded wip)
            if (typeof this._lastOrdinal === 'undefined') {
                const rootLastPath = this.tableFileMap.get(highestBucket).get('root');
                const lastMetadata = await ArrowBatchProtocol.readFileMetadata(rootLastPath);
                const lastTable = await ArrowBatchProtocol.readArrowBatchTable(
                    rootLastPath, lastMetadata, lastMetadata.batches.length - 1
                );

                if (lastTable.numRows > 0) {
                    const lastRow = lastTable.get(lastTable.numRows - 1).toArray();
                    this._lastOrdinal = lastRow[0];
                }
            }
        }
    }
    beginFlush() {
        // make sure auxiliary is empty (block concurrent flushes)
        if (this.auxiliarySize != 0)
            throw new Error(`beginFlush called but auxiliary buffers not empty, is system overloaded?`)

        // push intermediate to auxiliary and clear it
        this._initIntermediate();
    }

    updateOrdinal(ordinal: number | bigint) {
        // first validate ordering
        ordinal = BigInt(ordinal);

        if (!this.isFirstUpdate) {
            const expected = this._lastOrdinal + 1n;
            if (ordinal != expected)
                throw new Error(`Expected argument ordinal to be ${expected.toLocaleString()} but was ${ordinal.toLocaleString()}`)
        } else
            this.isFirstUpdate = false;

        this._lastOrdinal = ordinal;

        if (!this._firstOrdinal)
            this._firstOrdinal = this._lastOrdinal;

        // maybe start flush
        if (this.intermediateSize === Number(this.config.dumpSize))
            this.beginFlush();
    }

    protected getColumn(tableName: string, columnName: string, auxiliary: boolean = false) {
        const tableBuffers = auxiliary ? this._auxiliaryBuffers : this._intermediateBuffers;
        return tableBuffers.get(tableName).columns.get(columnName);
    }

    get auxiliarySize(): number {
        return this.getColumn('root', this.definition.root.ordinal, true).length;
    }

    get auxiliaryLastOrdinal(): bigint {
        return this.getColumn('root', this.definition.root.ordinal, true)[this.auxiliarySize - 1];
    }

    get intermediateSize(): number {
        return this.getColumn('root', this.definition.root.ordinal).length;
    }

    get intermediateLastOrdinal(): bigint {
        return this.getColumn('root', this.definition.root.ordinal)[this.intermediateSize - 1];
    }

    get cacheSize(): number {
        return this.cache.size;
    }

    /*
     * Get all rows from other buffers that reference this table's field with `ref` value
     */
    private getRowsFromBufferByRef(
        referencedTable: string,
        referencedField: ArrowTableMapping,
        ref: any,
        auxiliary: boolean = false
    ): {[key: string]: any[][]} {
        const tableBuffers = auxiliary ? this._auxiliaryBuffers : this._intermediateBuffers;

        const rootFieldIndex = this.tableMappings.get(referencedTable).findIndex(
            m => m.name === referencedField.name);

        if (rootFieldIndex == -1)
            throw new Error(`No field named ${referencedField.name} on ${referencedTable}`);

        const references = this.refMappings.get(referencedTable);

        const refs = {};
        for (const [tableName, tableBuff] of tableBuffers.entries()) {

            // make sure reference is to selected parent field
            if (!(tableName in references) ||
                references[tableName].childMapping.ref.field !== referencedField.name)
                continue;

            const rows = [];
            refs[tableName] = rows;

            const mappings = this.tableMappings.get(tableName);

            const refFieldIndex = mappings.findIndex(
                m => (
                    m.ref &&
                    m.ref.table === referencedTable &&
                    m.ref.field === referencedField.name
                ));

            if (refFieldIndex == -1)
                throw new Error(`No reference to ${referencedTable}::${referencedField.name} on ${tableName}`);

            const refField = mappings.at(refFieldIndex);

            let startIndex = -1;
            const refColumn = tableBuff.columns.get(refField.name);
            const indices = [];
            for (let i = 0; i < refColumn.length; i++) {
                if (refColumn[i] === ref) {
                    indices.push(i);

                    if (startIndex == -1)
                        startIndex = i;
                } else if (startIndex != -1)
                    break;
            }

            refs[tableName] = indices.map(
                i => mappings.map(
                    m => tableBuff.columns.get(m.name)[i]));
        }

        return refs;
    }

    /*
     * Get all rows from other tables that reference this table's field with `ref` value
     */
    private getRowsFromTablesByRef(
        referencedTable: string,
        referencedField: ArrowTableMapping,
        ref: any,
        tables: ArrowCachedTables
    ): {[key: string]: any[][]} {
        const rootFieldIndex = this.tableMappings.get(referencedTable).findIndex(
            m => m.name === referencedField.name);

        if (rootFieldIndex == -1)
            throw new Error(`No field named ${referencedField.name} on ${referencedTable}`);

        const references = this.refMappings.get(referencedTable);

        const refs = {};
        for (const [tableName, table] of Object.entries(tables.others)) {

            // make sure reference is to selected parent field
            if (tableName === referencedTable || !(tableName in references) ||
                references[tableName].childMapping.ref.field !== referencedField.name)
                continue;

            const rows = [];
            refs[tableName] = rows;

            const mappings = this.tableMappings.get(tableName);

            const refFieldIndex = this.tableMappings.get(tableName).findIndex(
                m => (
                    m.ref &&
                    m.ref.table === referencedTable &&
                    m.ref.field === referencedField.name
                ));

            if (refFieldIndex == -1)
                throw new Error(`No reference to ${referencedTable}::${referencedField.name} on ${tableName}`);

            let startIndex = -1;
            for (let i = 0; i < table.numRows; i++) {
                let row = table.get(i).toArray();
                row = mappings.map(
                    (m, j) => decodeRowValue(tableName, m, row[j]));

                if (row[refFieldIndex] === ref) {
                    rows.push(row);

                    if (startIndex == -1)
                        startIndex = i;
                } else if (startIndex != -1)
                    break;
            }
        }

        return refs;
    }

    private genRowWithRefsFromBuffers(
        tableName: string,
        row: any[],
        auxiliary: boolean = false
    ): RowWithRefs {
        const references  = this.refMappings.get(tableName);
        const processedRefs = new Set();
        const uniqueRefs = [];
        Object.values(references).forEach(val => {
            const parentInfo = {
                index: val.parentIndex,
                mapping: val.parentMapping
            };
            if (!processedRefs.has(JSON.stringify(parentInfo))) {
                processedRefs.add(JSON.stringify(parentInfo));
                uniqueRefs.push(val);
            }
        });

        const childRowMap = new Map<string, RowWithRefs[]>();
        for (const [refName, reference] of Object.entries(references)) {
            const refs = this.getRowsFromBufferByRef(
                tableName, reference.parentMapping, row[reference.parentIndex], auxiliary);

            for (const [childName, childRows] of Object.entries(refs)) {
                const key: string = childName;
                let rowContainer: RowWithRefs[] = [];
                if (!childRowMap.has(key))
                    childRowMap.set(key, rowContainer);
                else
                    rowContainer = childRowMap.get(key);
                for (const childRow of childRows)
                    rowContainer.push(this.genRowWithRefsFromBuffers(childName, childRow, auxiliary))
            }

        }
        return {
            row,
            refs: childRowMap
        };
    }

    private genRowWithRefsFromTables(
        tableName: string,
        row: any[],
        tables: ArrowCachedTables
    ): RowWithRefs {
        const references  = this.refMappings.get(tableName);
        const processedRefs = new Set();
        const uniqueRefs = [];
        Object.values(references).forEach(val => {
            const parentInfo = {
                index: val.parentIndex,
                mapping: val.parentMapping
            };
            if (!processedRefs.has(JSON.stringify(parentInfo))) {
                processedRefs.add(JSON.stringify(parentInfo));
                uniqueRefs.push(val);
            }
        });

        const childRowMap = new Map<string, RowWithRefs[]>();
        for (const reference of uniqueRefs) {
            const refs = this.getRowsFromTablesByRef(
                tableName, reference.parentMapping, row[reference.parentIndex], tables);

            for (const [childName, childRows] of Object.entries(refs)) {
                const key: string = childName;
                let rowContainer: RowWithRefs[] = [];
                if (!childRowMap.has(key))
                    childRowMap.set(key, rowContainer);
                else
                    rowContainer = childRowMap.get(key);
                for (const childRow of childRows)
                    rowContainer.push(this.genRowWithRefsFromTables(childName, childRow, tables))
            }

        }
        return {
            row,
            refs: childRowMap
        };
    }

    protected getBufferRow(tableName: string, index: number, auxiliary: boolean = false) {
        const tableBuffers = auxiliary ? this._auxiliaryBuffers : this._intermediateBuffers;
        const mappings = this.tableMappings.get(tableName);
        const tableBuff = tableBuffers.get(tableName);
        return mappings.map(
            m => tableBuff.columns.get(m.name)[index]);
    }
    async getRow(ordinal: bigint): Promise<RowWithRefs> {
        const ordinalField = this.definition.root.ordinal;

        // is row in intermediate buffers?
        const rootInterBuffs = this._intermediateBuffers.get('root');
        if (rootInterBuffs.columns.get(ordinalField).length > 0) {
            const oldestOnIntermediate = rootInterBuffs.columns.get(ordinalField)[0];
            const isOnIntermediate = ordinal >= oldestOnIntermediate && ordinal <= this.intermediateLastOrdinal
            if (isOnIntermediate) {
                const index = Number(ordinal - oldestOnIntermediate);
                const row = this.getBufferRow('root', index);
                return this.genRowWithRefsFromBuffers('root', row);
            }
        }

        // is row in auxiliary buffers?
        const rootAuxBuffs = this._auxiliaryBuffers.get('root');
        if (rootAuxBuffs.columns.get(ordinalField).length > 0) {
            const oldestOnAuxiliary = rootAuxBuffs.columns.get(ordinalField)[0];
            const isOnAuxiliary = ordinal >= oldestOnAuxiliary && ordinal <= this.auxiliaryLastOrdinal
            if (isOnAuxiliary) {
                const index = Number(ordinal - oldestOnAuxiliary);
                const row = this.getBufferRow('root', index, true);
                return this.genRowWithRefsFromBuffers('root', row, true);
            }
        }

        // is row on disk?
        const tables = await this.cache.getTablesFor(ordinal);

        if (!(isCachedTables(tables)))
            throw new Error(`Tables for ordinal ${ordinal} not found`);

        // fetch requested row from root table
        const adjustedOrdinal = this.getOrdinal(ordinal);
        const [bucketMetadata, _] = await this.cache.getMetadataFor(adjustedOrdinal, 'root');
        const relativeIndex = ordinal - bucketMetadata.startRow[0];
        const tableIndex = Number(relativeIndex % BigInt(this.config.dumpSize));
        const structRow = tables.root.get(tableIndex);

        if (!structRow)
            throw new Error(`Could not find row ${tableIndex}!`);

        const row = structRow.toArray();
        this.tableMappings.get('root').forEach((m, i) => {
            row[i] = decodeRowValue('root', m, row[i]);
        });

        return this.genRowWithRefsFromTables('root', row, tables);
    }

    iter(params: {from: bigint, to: bigint}) : RowScroller {
        return new RowScroller(this, params);
    }
}

export class RowScroller {

    private _isDone: boolean;
    readonly from: bigint;               // will push rows with ord >= `from`
    readonly to: bigint;                 // will stop pushing rows when row with ord `to` is reached

    protected reader: ArrowBatchReader;

    private _lastYielded: bigint;

    constructor(
        reader: ArrowBatchReader,
        params: {
            from: bigint,
            to: bigint
        }
    ) {
        this.reader = reader;
        this.from = params.from;
        this.to = params.to;
        this._lastYielded = this.from - 1n;
    }

    async nextResult(): Promise<RowWithRefs> {
        const nextBlock = this._lastYielded + 1n;
        const row = await this.reader.getRow(nextBlock);
        this._lastYielded = nextBlock;
        this._isDone = this._lastYielded == this.to;
        return row;
    }

    async *[Symbol.asyncIterator](): AsyncIterableIterator<RowWithRefs> {
        do {
            const row = await this.nextResult();
            yield row;
        } while (!this._isDone)
    }
}