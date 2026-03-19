import Redis, { RedisOptions } from 'ioredis';

export interface VSearchResult {
    key: string;
    similarity: number;
    metadata?: Record<string, unknown>;
}

export interface VSearchOptions {
    k: number;
    filter?: { key: string; value: string };
    meta?: boolean;
}

export class Lux extends Redis {
    constructor(options?: RedisOptions | string) {
        super(options as any);
    }

    async vset(
        key: string,
        vector: number[],
        options?: { metadata?: Record<string, unknown>; ex?: number; px?: number }
    ): Promise<'OK'> {
        const args: (string | number)[] = [key, vector.length, ...vector];
        if (options?.metadata) {
            args.push('META', JSON.stringify(options.metadata));
        }
        if (options?.ex) {
            args.push('EX', options.ex);
        } else if (options?.px) {
            args.push('PX', options.px);
        }
        return this.call('VSET', ...args) as Promise<'OK'>;
    }

    async vget(key: string): Promise<{ dims: number; vector: number[]; metadata?: Record<string, unknown> } | null> {
        const result = (await this.call('VGET', key)) as any[] | null;
        if (!result || !Array.isArray(result)) return null;

        const dims = parseInt(result[0], 10);
        const vector: number[] = [];
        for (let i = 1; i <= dims; i++) {
            vector.push(parseFloat(result[i]));
        }
        const metaRaw = result[dims + 1];
        let metadata: Record<string, unknown> | undefined;
        if (metaRaw) {
            try {
                metadata = JSON.parse(metaRaw);
            } catch {}
        }
        return { dims, vector, metadata };
    }

    async vsearch(query: number[], options: VSearchOptions): Promise<VSearchResult[]> {
        const args: (string | number)[] = [query.length, ...query, 'K', options.k];
        if (options.filter) {
            args.push('FILTER', options.filter.key, options.filter.value);
        }
        if (options.meta) {
            args.push('META');
        }
        const result = (await this.call('VSEARCH', ...args)) as any[] | null;
        if (!result || !Array.isArray(result)) return [];

        const results: VSearchResult[] = [];
        for (const item of result) {
            if (Array.isArray(item)) {
                const entry: VSearchResult = {
                    key: item[0],
                    similarity: parseFloat(item[1]),
                };
                if (options.meta && item[2]) {
                    try {
                        entry.metadata = JSON.parse(item[2]);
                    } catch {
                        entry.metadata = { _raw: item[2] };
                    }
                }
                results.push(entry);
            }
        }
        return results;
    }

    async vcard(): Promise<number> {
        return this.call('VCARD') as Promise<number>;
    }
}

export default Lux;
