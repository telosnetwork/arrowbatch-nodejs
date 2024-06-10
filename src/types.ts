import z from 'zod';
import bytes from 'bytes';

import {ArrowBatchCompression, DEFAULT_BUCKET_SIZE, DEFAULT_DUMP_SIZE, DEFAULT_STREAM_BUF_MEM} from "./protocol.js";
import {DEFAULT_BROADCAST_HOST, DEFAULT_BROADCAST_PORT} from "./writer/index.js";

const BigIntSchema = z.union([
    z.string(),
    z.number(),
    z.bigint()
]).transform((val, ctx) => {
    try {
        return BigInt(val);
    } catch {
        ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: 'Invalid bigint',
        });
        return z.NEVER;
    }
});

const BufferSizeSchema = z.union([
    z.string().transform((val, ctx) => {
        const numericValue = bytes(val);
        if (typeof numericValue !== 'number' || isNaN(numericValue)) {
            ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'Invalid memory buffer size format',
            });
            return z.NEVER;
        }
        return numericValue;
    }),
    z.number()
]);

export const ArrowBatchConfigSchema = z.object({
    dataDir: z.string(),

    liveMode: z.boolean().default(false),
    wsHost: z.string().default(DEFAULT_BROADCAST_HOST),
    wsPort: z.number().default(DEFAULT_BROADCAST_PORT),
    writerLogLevel: z.string().default('warning'),
    broadcastLogLevel: z.string().default('warning'),
    bucketSize: BigIntSchema.default(DEFAULT_BUCKET_SIZE),
    dumpSize: BigIntSchema.default(DEFAULT_DUMP_SIZE),
    compression: z.nativeEnum(ArrowBatchCompression).default(ArrowBatchCompression.ZSTD),

    writeStreamSize: BufferSizeSchema.default(DEFAULT_STREAM_BUF_MEM)
});

export type ArrowBatchConfig = z.infer<typeof ArrowBatchConfigSchema>;
