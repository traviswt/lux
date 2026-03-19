"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Lux = void 0;
const ioredis_1 = __importDefault(require("ioredis"));
class Lux extends ioredis_1.default {
    constructor(options) {
        super(options);
    }
    async vset(key, vector, options) {
        const args = [key, vector.length, ...vector];
        if (options?.metadata) {
            args.push('META', JSON.stringify(options.metadata));
        }
        if (options?.ex) {
            args.push('EX', options.ex);
        }
        else if (options?.px) {
            args.push('PX', options.px);
        }
        return this.call('VSET', ...args);
    }
    async vget(key) {
        const result = (await this.call('VGET', key));
        if (!result || !Array.isArray(result))
            return null;
        const dims = parseInt(result[0], 10);
        const vector = [];
        for (let i = 1; i <= dims; i++) {
            vector.push(parseFloat(result[i]));
        }
        const metaRaw = result[dims + 1];
        let metadata;
        if (metaRaw) {
            try {
                metadata = JSON.parse(metaRaw);
            }
            catch { }
        }
        return { dims, vector, metadata };
    }
    async vsearch(query, options) {
        const args = [query.length, ...query, 'K', options.k];
        if (options.filter) {
            args.push('FILTER', options.filter.key, options.filter.value);
        }
        if (options.meta) {
            args.push('META');
        }
        const result = (await this.call('VSEARCH', ...args));
        if (!result || !Array.isArray(result))
            return [];
        const results = [];
        for (const item of result) {
            if (Array.isArray(item)) {
                const entry = {
                    key: item[0],
                    similarity: parseFloat(item[1]),
                };
                if (options.meta && item[2]) {
                    try {
                        entry.metadata = JSON.parse(item[2]);
                    }
                    catch {
                        entry.metadata = { _raw: item[2] };
                    }
                }
                results.push(entry);
            }
        }
        return results;
    }
    async vcard() {
        return this.call('VCARD');
    }
}
exports.Lux = Lux;
exports.default = Lux;
