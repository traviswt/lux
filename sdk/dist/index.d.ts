import Redis, { RedisOptions } from 'ioredis';
export interface VSearchResult {
    key: string;
    similarity: number;
    metadata?: Record<string, unknown>;
}
export interface VSearchOptions {
    k: number;
    filter?: {
        key: string;
        value: string;
    };
    meta?: boolean;
}
export declare class Lux extends Redis {
    constructor(options?: RedisOptions | string);
    vset(key: string, vector: number[], options?: {
        metadata?: Record<string, unknown>;
        ex?: number;
        px?: number;
    }): Promise<'OK'>;
    vget(key: string): Promise<{
        dims: number;
        vector: number[];
        metadata?: Record<string, unknown>;
    } | null>;
    vsearch(query: number[], options: VSearchOptions): Promise<VSearchResult[]>;
    vcard(): Promise<number>;
}
export default Lux;
