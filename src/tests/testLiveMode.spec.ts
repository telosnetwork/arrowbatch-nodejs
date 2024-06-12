import {
    ArrowBatchConfig, ArrowBatchConfigSchema,
    ArrowBatchReader,
    createLogger, FlushReq, sleep
} from '../index.js';

describe('liveMode', () => {

    const logger = createLogger('testLive', 'debug');

    const config: ArrowBatchConfig = ArrowBatchConfigSchema.parse({
        dataDir: '../telosevm-translator/arrow-data',
        // writerLogLevel: 'debug',
        liveMode: true
    });

    it('connect and get info', async () => {
        const reader = new ArrowBatchReader(config, undefined, logger);
        function onFlushHandler (flushInfo: FlushReq['params']) {
            logger.info(`writer flushed: ${JSON.stringify(flushInfo)}`);
        }

        await reader.init(0);
        await reader.beginSync(
            5000, {onFlush: onFlushHandler}
        )

        await sleep(2 * 60 * 1000);
    });
});
