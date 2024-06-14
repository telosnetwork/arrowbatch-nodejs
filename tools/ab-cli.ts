#!/usr/bin/env node
import { program } from 'commander';
import * as fs from 'fs';
import {
    ArrowBatchConfig,
    ArrowBatchConfigSchema,
    ArrowBatchReader,
    createLogger,
    extendedStringify, humanizeByteSize,
    packageInfo
} from "../src/index.js";
import * as process from "node:process";
import * as console from "node:console";
import fastFolderSizeSync from "fast-folder-size/sync.js";

async function readerFromCLIOpts(options: {
    config: string, dataDir: string
}): Promise<ArrowBatchReader> {
    let config: ArrowBatchConfig;
    if (options.config) {
        // Check if the config file exists
        if (!fs.existsSync(options.config)) {
            console.error(`Config file '${options.config}' does not exist.`);
            process.exit(1);
        }

        // Read the config file
        const configData = JSON.parse(fs.readFileSync(options.config, 'utf8'));
        config = ArrowBatchConfigSchema.parse(configData);
    } else if (options.dataDir) {
        config = ArrowBatchConfigSchema.parse({
            dataDir: options.dataDir
        });
    } else {
        console.error(`Cant figure out data dir with those arguments. Try --help`);
        process.exit(1);
    }

    // Check if the data directory exists
    if (!fs.existsSync(config.dataDir)) {
        console.error(`Data directory '${config.dataDir}' does not exist.`);
        process.exit(1);
    }

    const logger = createLogger('ab-cli', 'info');
    const reader = new ArrowBatchReader(config, undefined, logger);
    await reader.init();

    return reader;
}

program
    .version(packageInfo.version)
    .description('AB CLI Tool');

program
    .command('stat')
    .description('Get statistics for the data directory')
    .option('-c, --config <configFile>', 'Path to the config file', undefined)
    .option('-d, --data-dir <dataDir>', 'Path to data directory, generate config dynamically', undefined)
    .action(async (options: {config: string, dataDir: string}) => {
        const reader = await readerFromCLIOpts(options);

        const dataDirSize = fastFolderSizeSync(reader.config.dataDir);
        const totalRows = reader.lastOrdinal - reader.firstOrdinal;

        console.log(`data dir size: ${humanizeByteSize(dataDirSize)}`);

        console.log(`start ordinal: ${reader.firstOrdinal.toLocaleString()}`);
        console.log(`last ordinal: ${reader.lastOrdinal.toLocaleString()}`);
        console.log(`total rows: ${totalRows.toLocaleString()}`);
    });

program
    .command('validate')
    .description('Validate on disk tables')
    .option('-c, --config <configFile>', 'Path to the config file', undefined)
    .option('-d, --data-dir <dataDir>', 'Path to data directory, generate config dynamically', undefined)
    .action(async (options: {config: string, dataDir: string}) => {
        const reader = await readerFromCLIOpts(options);

        try {
            await reader.validate();
        } catch (e) {
            reader.logger.error(e.message);
            process.exit(1);
        }
    });

program
    .command('get <ordinal>')
    .description('Get the value at the specified ordinal')
    .option('-c, --config <configFile>', 'Path to the config file', undefined)
    .option('-d, --data-dir <dataDir>', 'Path to data directory, generate config dynamically', undefined)
    .action(async (ordinal: string, options: {config: string, dataDir: string}) => {
        const reader = await readerFromCLIOpts(options);

        const row = await reader.getRow(BigInt(ordinal));

        console.log(extendedStringify(row, 4));
    });

program.parse(process.argv);