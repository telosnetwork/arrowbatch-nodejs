import {
    ArrowBatchConfig, ArrowBatchConfigSchema,
    ArrowBatchReader,
    createLogger, sleep
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
        await reader.init(0);

        while(true)
            await sleep(1000);
    });
});
