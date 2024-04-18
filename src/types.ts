import z from 'zod';

import {ArrowBatchCompression} from "./protocol.js";

const BigIntSchema = z.union([
    z.string(),
    z.number(),
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

export const ArrowBatchConfigSchema = z.object({
    dataDir: z.string(),
    writerLogLevel: z.string().optional(),
    bucketSize: BigIntSchema,
    dumpSize: BigIntSchema.optional(),
    compression: z.nativeEnum(ArrowBatchCompression).optional(),
});

export type ArrowBatchConfig = z.infer<typeof ArrowBatchConfigSchema>;
