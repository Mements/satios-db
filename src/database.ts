import { Database, Statement } from "bun:sqlite";
// Import z from zod - no changes needed here for v4 basic usage
import { z } from "zod";
import path from "path";
import fs from "node:fs";
import { createHash } from "node:crypto";

// Define or import RESERVED_SQLITE_WORDS robustly
const RESERVED_SQLITE_WORDS = new Set(["ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE", "AND", "AS", "ASC", "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT", "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DO", "DROP", "EACH", "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUDE", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FILTER", "FIRST", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL", "GENERATED", "GLOB", "GROUP", "GROUPS", "HAVING", "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LAST", "LEFT", "LIKE", "LIMIT", "MATCH", "MATERIALIZED", "NATURAL", "NO", "NOT", "NOTHING", "NOTNULL", "NULL", "NULLS", "OF", "OFFSET", "ON", "OR", "ORDER", "OTHERS", "OUTER", "OVER", "PARTITION", "PLAN", "PRAGMA", "PRECEDING", "PRIMARY", "QUERY", "RAISE", "RANGE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RIGHT", "ROLLBACK", "ROW", "ROWS", "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION", "TRIGGER", "UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE", "WINDOW", "WITH", "WITHOUT"]);

// --- Base Types --- (No changes needed)
export type RawDatabaseRecord = {
    id: string;
    input: Record<string, any>;
    output?: Record<string, any>;
};
export type RangeFilter<T> = [T, T];
export type Filter<T extends z.ZodObject<any>> = Partial<z.infer<T> & {
    [K in keyof z.infer<T>]?: z.infer<T>[K] | RangeFilter<z.infer<T>[K]>
}>;
export interface TableNames {
    inputTable: string;
    outputTable: string;
    changesTable: string;
    metadataTable: string;
}

// --- Zod Type Guards (Updated for Zod v4) ---
// Replace z.ZodTypeAny with z.ZodType as per Zod v4 changes.
// The underlying _def.typeName check remains valid based on common Zod usage,
// though relying on internal properties like _def is generally less stable than public APIs.
// However, Zod doesn't provide a public API for this specific check as of v4.
function isZodString(value: z.ZodType): value is z.ZodString { return value._def.typeName === "ZodString"; }
function isZodNumber(value: z.ZodType): value is z.ZodNumber { return value._def.typeName === "ZodNumber"; }
function isZodBoolean(value: z.ZodType): value is z.ZodBoolean { return value._def.typeName === "ZodBoolean"; }
function isZodEnum(value: z.ZodType): value is z.ZodEnum<any> { return value._def.typeName === "ZodEnum"; }
function isZodDate(value: z.ZodType): value is z.ZodDate { return value._def.typeName === "ZodDate"; }
function isZodArray(value: z.ZodType): value is z.ZodArray<any> { return value._def.typeName === "ZodArray"; }
function isZodObject(value: z.ZodType): value is z.ZodObject<any> { return value._def.typeName === "ZodObject"; }
function isZodNullable(value: z.ZodType): value is z.ZodNullable<any> { return value._def.typeName === "ZodNullable"; }
function isZodOptional(value: z.ZodType): value is z.ZodOptional<any> { return value._def.typeName === "ZodOptional"; }
function isZodDefault(value: z.ZodType): value is z.ZodDefault<any> { return value._def.typeName === "ZodDefault"; }
// --- End Zod Type Guards ---

/**
 * Generates a deterministic SHA256 hash (substring) for a given input object.
 * Ensures consistent hashing by sorting object keys before stringification.
 * @param input The input object.
 * @param inputSchema The Zod schema for the input (used for type safety hint).
 * @returns A 16-character hex string ID.
 */
function generateRecordId<I extends z.ZodObject<any>>(input: z.infer<I>, inputSchema: I): string {
    // inputSchema.parse(input); // Optional safety check
    const orderedInput = Object.keys(input)
        .sort()
        .reduce((obj, key) => {
            let value = input[key];
            if (value !== null && typeof value === 'object') {
                try {
                    value = JSON.stringify(value, (k, v) => {
                        if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
                            return Object.keys(v).sort().reduce((sortedObj, innerKey) => {
                                sortedObj[innerKey] = v[innerKey]; return sortedObj;
                            }, {} as Record<string, any>);
                        } return v;
                    });
                } catch { value = JSON.stringify(value); }
            } else { value = JSON.stringify(value); }
            obj[key] = value;
            return obj;
        }, {} as Record<string, any>);
    return createHash('sha256').update(JSON.stringify(orderedInput)).digest('hex').substring(0, 16);
}

/**
 * Sanitizes a base name for use in SQLite table/trigger names.
 * Replaces invalid characters with underscores, prefixes with 't_' if starting with a digit,
 * and prefixes with 't_' if it matches a reserved SQL keyword.
 * @param name The base name to sanitize.
 * @returns A sanitized string suitable for use in SQLite identifiers.
 */
function sanitizeName(name: string): string {
    let sanitized = name.replace(/[^a-zA-Z0-9_]/g, "_");
    if (/^[0-9]/.test(sanitized)) sanitized = "t_" + sanitized;
    if (RESERVED_SQLITE_WORDS.has(sanitized.toUpperCase())) sanitized = "t_" + sanitized;
    return sanitized;
}

// --- Core Database Interface ---

/**
 * Represents the core database interaction layer. Provides methods for initializing,
 * inserting, querying, and managing the two-table structure.
 */
export interface SatiDbCore<I extends z.ZodObject<any>, O extends z.ZodObject<any>> {
    /** Initializes the database: creates tables, indexes, triggers, and verifies schemas. Must be called before other methods. */
    init: () => Promise<void>;
    /**
     * Inserts or updates a record. Ensures input exists. Inserts or replaces the output.
     * Ideal for storing latest state associated with an input (e.g., latest price).
     * @param input The input data object.
     * @param output The output data object to insert or replace.
     * @returns Promise resolving to an object containing the record `id`.
     */
    upsert: (input: z.infer<I>, output: z.infer<O>) => Promise<{ id: string }>; // <-- New Method
    /**
     * Inserts an input record and optionally an output record.
     * If input exists, does nothing for input.
     * If output is provided and an output for this ID already exists, it's ignored (write-once).
     * @param input The input data object, conforming to the input schema.
     * @param output Optional output data object, conforming to the output schema.
     * @returns Promise resolving to an object containing the generated/existing `id` and whether the `output` was ignored.
     */
    insert: (input: z.infer<I>, output?: z.infer<O>) => Promise<{ id: string, output_ignored?: boolean }>;
    /**
     * Finds raw database records matching the given filter criteria.
     * Performs a LEFT JOIN between input and output tables.
     * Returns raw, deserialized data (no Zod parsing for performance).
     * @param filter Optional filter object with `input` and/or `output` criteria.
     * @returns Promise resolving to an array of RawDatabaseRecord objects.
     */
    findRaw: (filter?: { input?: Filter<I>, output?: Filter<O> }) => Promise<Array<RawDatabaseRecord>>;
    /**
     * Finds a single raw database record by its exact input hash ID.
     * Returns raw, deserialized data (no Zod parsing for performance).
     * @param id The unique ID (input hash) of the record.
     * @returns Promise resolving to a RawDatabaseRecord or null if not found.
     */
    findByIdRaw: (id: string) => Promise<RawDatabaseRecord | null>;
    /**
     * Attempts to add an output to an existing input record.
     * Throws an error if the input record doesn't exist or if an output already exists for that input ID.
     * Use this only if you specifically need to add output later and guarantee it wasn't already added.
     * @param input The complete input object (used to find the record ID).
     * @param output The partial or complete output data to add.
     * @returns Promise resolving to an object containing the record `id`.
     */
    updateOutput: (input: z.infer<I>, output: Partial<z.infer<O>>) => Promise<{ id: string }>;
    /**
     * Deletes an input record and its corresponding output record (via CASCADE).
     * Requires the complete input object to derive the ID.
     * @param input The complete input object corresponding to the record to delete.
     * @returns Promise resolving to an object containing the `id` of the deleted record.
     * @throws Error if the record is not found.
     */
    delete: (input: z.infer<I>) => Promise<{ id: string }>;
    /**
     * Executes a raw SQL query with parameters. Use with caution.
     * @param sql The SQL query string. Use placeholders (?) for parameters.
     * @param params Parameters to bind to the query placeholders.
     * @returns Promise resolving to an array of results. Type T needs to be specified by the caller.
     */
    query: <T = any>(sql: string, ...params: any[]) => Promise<T[]>;
    /** Gets the Zod input schema associated with this database instance. */
    getInputSchema: () => I;
    /** Gets the Zod output schema associated with this database instance. */
    getOutputSchema: () => O;
    /** Gets the original base name provided for this database instance. */
    getName: () => string;
    /** Closes the database connection. */
    close: () => void;
    /** Internal helpers exposed for potential use by higher-level libraries (like agent.ts). */
    _internal: {
        /** Generates the deterministic ID for a given input object. */
        generateId: (input: z.infer<I>) => string;
        /** Gets the underlying bun:sqlite Database instance. */
        getDbInstance: () => Database;
        /** Gets the names of the tables used by this instance. */
        getTableNames: () => TableNames;
        /** Checks if the database instance has been initialized. */
        isInitialized: () => boolean;
    };
}


// --- useDatabase Implementation ---
export function useDatabase<I extends z.ZodObject<any>, O extends z.ZodObject<any>>(
    dbPath: string,
    inputSchema: I,
    outputSchema: O
): SatiDbCore<I, O> {
    const baseName = path.basename(dbPath, path.extname(dbPath));
    const sanitizedName = sanitizeName(baseName);
    const tableNames: TableNames = {
        inputTable: `input_${sanitizedName}`,
        outputTable: `output_${sanitizedName}`,
        changesTable: `_changes_${sanitizedName}`,
        metadataTable: `_sati_metadata_${sanitizedName}`
    };

    if ('id' in inputSchema.shape || 'id' in outputSchema.shape) {
        throw new Error(`[Database Setup ${baseName}] Schemas cannot contain 'id'.`);
    }

    const dir = path.dirname(dbPath);
    try { fs.mkdirSync(dir, { recursive: true }); } catch (error: any) { if (error.code !== 'EEXIST') throw error; }

    const db = new Database(dbPath);
    try { db.exec("PRAGMA journal_mode = WAL;"); } catch (e: any) { console.warn(`[Database ${baseName}] Could not enable WAL mode:`, e.message); }
    let initialized = false;

    // --- Internal Utility Functions (Updated for Zod v4) ---

    // Replace z.ZodTypeAny with z.ZodType
    function getSqliteType(zodType: z.ZodType): string | null {
        const unwrapped = zodType.unwrap ? zodType.unwrap() : zodType;
        if (isZodString(unwrapped) || isZodEnum(unwrapped) || isZodArray(unwrapped) || isZodObject(unwrapped) || isZodDate(unwrapped)) return "TEXT";
        if (isZodNumber(unwrapped)) return "NUMERIC";
        if (isZodBoolean(unwrapped)) return "INTEGER";
        // Handle Zod v4 stringbool if needed - treated as boolean/INTEGER here
        // Note: z.stringbool() is a Zod v4 addition. If you use it in your schemas,
        // it should probably map to INTEGER (0/1) like ZodBoolean for database storage.
        // Add check if necessary: if (unwrapped._def.typeName === "ZodStringBoolean") return "INTEGER";
        if (isZodDefault(zodType)) return getSqliteType(zodType._def.innerType);
        console.warn(`[Database Schema ${baseName}] Unsupported/Unmapped Zod type: ${zodType._def.typeName}`);
        return "TEXT"; // Default fallback
    }

    // Replace z.ZodTypeAny with z.ZodType
    function getDefaultValueClause(zodType: z.ZodType): string {
        if (isZodDefault(zodType)) {
            const defaultValue = typeof zodType._def.defaultValue === 'function' ? zodType._def.defaultValue() : zodType._def.defaultValue;
            if (defaultValue === undefined || defaultValue === null) return "";
            let formattedDefault: string | number | null = null;
            if (typeof defaultValue === 'string') formattedDefault = `'${defaultValue.replace(/'/g, "''")}'`;
            else if (typeof defaultValue === 'number') formattedDefault = defaultValue;
            else if (typeof defaultValue === 'boolean') formattedDefault = defaultValue ? 1 : 0;
            else if (defaultValue instanceof Date) formattedDefault = `'${defaultValue.toISOString()}'`;
            if (formattedDefault !== null) return ` DEFAULT ${formattedDefault}`;
        } return "";
    }

    // No Zod v4 changes needed here, relies on getSqliteType and getDefaultValueClause which were updated
    function createTableSql(schema: z.ZodObject<any>, tableName: string, isOutputTable: boolean = false): string {
        const columns = ["id TEXT PRIMARY KEY"];
        for (const [key, valueDef] of Object.entries(schema.shape)) {
            let columnType = getSqliteType(valueDef);
            if (!columnType) continue;
            const isOptional = valueDef.isOptional() || valueDef.isNullable();
            const defaultClause = getDefaultValueClause(valueDef);
            // Logic for NULL/NOT NULL based on optional/nullable/default remains the same
            const nullClause = (isOutputTable || (isOptional && !isZodDefault(valueDef))) ? " NULL" : (defaultClause ? "" : " NOT NULL");
            columns.push(`${key} ${columnType}${nullClause}${defaultClause}`);
        }
        const fkConstraint = isOutputTable ? `, FOREIGN KEY(id) REFERENCES ${tableNames.inputTable}(id) ON DELETE CASCADE` : "";
        return `CREATE TABLE IF NOT EXISTS ${tableName} (${columns.join(", ")}${fkConstraint})`;
    }

    // No Zod v4 changes needed here, relies on getSqliteType which was updated
    async function createAutomaticIndexes(dbInstance: Database, tblName: string, schema: z.ZodObject<any>): Promise<void> {
        if (!dbInstance || dbInstance.closed) return;
        try {
            const tableInfo: { name: string }[] = dbInstance.query(`PRAGMA table_info(${tblName})`).all() as any;
            for (const column of tableInfo) {
                if (column.name === "id" || !schema.shape[column.name]) continue;
                const zodType = schema.shape[column.name];
                const sqlType = getSqliteType(zodType);
                // Indexing logic remains the same
                if (sqlType === "NUMERIC" || sqlType === "INTEGER" || isZodEnum(zodType) || isZodDate(zodType)) {
                    const indexName = `idx_${tblName}_${column.name}`;
                    try { dbInstance.query(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${tblName}(${column.name})`).run(); }
                    catch (error: any) { console.warn(`[DB Indexing ${baseName}] Failed index ${indexName}:`, error?.message); }
                }
            }
        } catch (error: any) { if (!error.message.includes("no such table")) { console.error(`[DB Indexing ${baseName}] Error table info ${tblName}:`, error?.message); } }
    }

    // Replace z.ZodTypeAny with z.ZodType
    function processValueForStorage(value: any, zodType: z.ZodType): any {
        if (value === undefined || value === null) return null;
        // .unwrap() usage likely still correct in v4 for handling optional/nullable/default
        const unwrapped = zodType.unwrap ? zodType.unwrap() : zodType;
        if (value instanceof Date) return value.toISOString();
        if (isZodBoolean(unwrapped)) return value ? 1 : 0;
        // Add handling for Zod v4 stringbool if you use it in schemas
        // if (unwrapped._def.typeName === "ZodStringBoolean") return value ? 1 : 0; // Assuming true maps to 1, false to 0
        if (isZodArray(unwrapped) || isZodObject(unwrapped)) return JSON.stringify(value);
        if (isZodNumber(unwrapped) && typeof value === 'number' && !Number.isFinite(value)) { return null; } // Handle NaN/Infinity
        return value;
    }

    // No Zod v4 changes needed here, relies on processValueForStorage which was updated
    function processObjectForStorage(data: Record<string, any>, schema: z.ZodObject<any>): Record<string, any> {
        const result: Record<string, any> = {};
        for (const [key, value] of Object.entries(data)) {
            if (schema.shape[key]) { result[key] = processValueForStorage(value, schema.shape[key]); }
        } return result;
    }

    // Replace z.ZodTypeAny with z.ZodType
    function deserializeValue(value: any, zodType: z.ZodType): any {
        if (value === null) return null;
        const unwrapped = zodType.unwrap ? zodType.unwrap() : zodType;
        try {
            if (isZodDate(unwrapped)) return new Date(value);
            if (isZodBoolean(unwrapped)) return Boolean(Number(value));
             // Add handling for Zod v4 stringbool if needed (deserializes from INTEGER 0/1)
            // if (unwrapped._def.typeName === "ZodStringBoolean") return Boolean(Number(value));
            if (isZodArray(unwrapped) || isZodObject(unwrapped)) return typeof value === 'string' ? JSON.parse(value) : value;
            if (isZodNumber(unwrapped)) return Number(value);
            if (isZodString(unwrapped) || isZodEnum(unwrapped)) return String(value);
        } catch (e: any) { console.error(`[DB Deserialization ${baseName}] Error value "${value}" type ${unwrapped?._def?.typeName}:`, e?.message); return undefined; } // Return undefined on error? Or null?
        return value; // Fallback for types not explicitly handled
    }

    // No Zod v4 changes needed here, relies on deserializeValue which was updated
    function mapRowToRawRecord(row: any): RawDatabaseRecord | null {
        if (!row || typeof row.id !== 'string') return null;
        const rawInputData: Record<string, any> = {};
        const rawOutputData: Record<string, any> = {};
        let hasOutputField = false;
        for (const [key, dbValue] of Object.entries(row)) {
            if (key === 'id') continue; // Avoid duplicate ID if selecting i.*, o.*
            if (key in inputSchema.shape) {
                rawInputData[key] = deserializeValue(dbValue, inputSchema.shape[key]);
            } else if (key in outputSchema.shape) {
                const deserialized = deserializeValue(dbValue, outputSchema.shape[key]);
                // Check if *any* output field was non-null after deserialization
                if (dbValue !== null) { // Check original DB value was not NULL
                    hasOutputField = true;
                    rawOutputData[key] = deserialized;
                 } else {
                     rawOutputData[key] = null; // Explicitly set null if DB was null
                 }
            }
        }
         // Only include output object if there were non-null values for it in the DB row
        return { id: row.id, input: rawInputData, ...(hasOutputField && { output: rawOutputData }) };
    }

    // No Zod v4 changes needed here, relies on processValueForStorage which was updated
    function buildWhereClause(filter: { input?: Filter<I>, output?: Filter<O> } | undefined, params: any[], inputAlias: string = 'i', outputAlias: string = 'o'): string {
        let clauses: string[] = [];
        const addFilters = (dataFilter: Record<string, any> | undefined, schema: z.ZodObject<any>, alias: string) => {
            if (!dataFilter) return;
            for (let [key, value] of Object.entries(dataFilter)) {
                if (value === undefined || !schema.shape[key]) continue;
                const zodType = schema.shape[key];
                if (Array.isArray(value) && value.length === 2 && !isZodArray(zodType)) { // Range filter
                    const [min, max] = value;
                    clauses.push(`${alias}.${key} BETWEEN ? AND ?`);
                    params.push(processValueForStorage(min, zodType), processValueForStorage(max, zodType));
                } else { // Direct equality filter
                    clauses.push(`${alias}.${key} = ?`);
                    params.push(processValueForStorage(value, zodType));
                }
            }
        };
        addFilters(filter?.input, inputSchema, inputAlias);
        addFilters(filter?.output, outputSchema, outputAlias);
        return clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "";
    }

    // No Zod v4 changes needed here, relies on getSqliteType/getDefaultValueClause which were updated
    function ensureTableColumns(dbInstance: Database, tableName: string, schema: z.ZodObject<any>, isOutputTable: boolean = false) {
        if (!dbInstance || dbInstance.closed) return;
        try {
            const tableInfo: { name: string }[] = dbInstance.query(`PRAGMA table_info(${tableName})`).all() as any;
            const existingColumns = new Set(tableInfo.map(col => col.name));
            for (const [key, valueDef] of Object.entries(schema.shape)) {
                if (key === 'id') continue; // Skip primary key
                if (!existingColumns.has(key)) {
                    let columnType = getSqliteType(valueDef);
                    if (columnType) {
                        const defaultClause = getDefaultValueClause(valueDef);
                        const isOptional = valueDef.isOptional() || valueDef.isNullable();
                        const nullClause = (isOutputTable || (isOptional && !isZodDefault(valueDef))) ? " NULL" : (defaultClause ? "" : " NOT NULL");
                        const addColumnSql = `ALTER TABLE ${tableName} ADD COLUMN ${key} ${columnType}${nullClause}${defaultClause}`;
                        dbInstance.query(addColumnSql).run();
                        console.log(`[DB Schema ${baseName}] Added column ${key} to ${tableName}`);
                    } else { console.warn(`[DB Schema ${baseName}] Could not determine SQLite type for new column ${key}`); }
                }
            }
        } catch (error: any) { if (!error.message.includes("no such table")) { console.error(`[DB Schema ${baseName}] Error ensuring columns for ${tableName}:`, error?.message); throw error; } }
    }

    // No Zod v4 changes needed in trigger logic
    const setupTriggers = () => {
        if (!db || db.closed) return;
        db.transaction(() => {
            db.query(`CREATE TABLE IF NOT EXISTS ${tableNames.changesTable} (change_id INTEGER PRIMARY KEY AUTOINCREMENT, record_id TEXT NOT NULL, change_type TEXT NOT NULL, table_name TEXT NOT NULL, changed_at INTEGER NOT NULL DEFAULT (unixepoch()))`).run();
            // Input Triggers
            db.query(`DROP TRIGGER IF EXISTS ${tableNames.inputTable}_insert_trigger`).run();
            db.query(`CREATE TRIGGER ${tableNames.inputTable}_insert_trigger AFTER INSERT ON ${tableNames.inputTable} BEGIN INSERT INTO ${tableNames.changesTable} (record_id, change_type, table_name) VALUES (NEW.id, 'input_added', 'input'); END;`).run();
            db.query(`DROP TRIGGER IF EXISTS ${tableNames.inputTable}_delete_trigger`).run();
            db.query(`CREATE TRIGGER ${tableNames.inputTable}_delete_trigger AFTER DELETE ON ${tableNames.inputTable} BEGIN INSERT INTO ${tableNames.changesTable} (record_id, change_type, table_name) VALUES (OLD.id, 'record_deleted', 'input'); END;`).run();
            // Output Triggers
            db.query(`DROP TRIGGER IF EXISTS ${tableNames.outputTable}_insert_trigger`).run();
            db.query(`CREATE TRIGGER ${tableNames.outputTable}_insert_trigger AFTER INSERT ON ${tableNames.outputTable} BEGIN INSERT INTO ${tableNames.changesTable} (record_id, change_type, table_name) VALUES (NEW.id, 'output_added', 'output'); END;`).run();
            db.query(`DROP TRIGGER IF EXISTS ${tableNames.outputTable}_update_trigger`).run();
            db.query(`CREATE TRIGGER ${tableNames.outputTable}_update_trigger AFTER UPDATE ON ${tableNames.outputTable} BEGIN INSERT INTO ${tableNames.changesTable} (record_id, change_type, table_name) VALUES (NEW.id, 'record_updated', 'output'); END;`).run();
        })();
    };

    // No Zod v4 changes needed for schema shape stringification (uses _def.typeName)
    const stringifySchemaShape = (schema: z.ZodObject<any>): string => {
        const shape = schema.shape;
        const sortedKeys = Object.keys(shape).sort();
        const shapeRepresentation: Record<string, string> = {};
        for (const key of sortedKeys) {
            // Use _def.typeName which is generally available
            shapeRepresentation[key] = shape[key]._def.typeName;
        }
        return JSON.stringify(shapeRepresentation);
    };

    // --- Public API Implementation ---
    const coreApi: SatiDbCore<I, O> = {
        async init(): Promise<void> {
            // Init logic remains the same, relies on createTableSql, ensureTableColumns, etc.
            if (initialized || !db || db.closed) return;
            db.transaction(() => {
                // Create core tables
                db.query(createTableSql(inputSchema, tableNames.inputTable, false)).run();
                db.query(createTableSql(outputSchema, tableNames.outputTable, true)).run();
                db.query(`CREATE TABLE IF NOT EXISTS ${tableNames.metadataTable} (key TEXT PRIMARY KEY, value TEXT NOT NULL)`).run();

                // Verify or store schema shape
                const currentInputShapeStr = stringifySchemaShape(inputSchema);
                const currentOutputShapeStr = stringifySchemaShape(outputSchema);
                const storedInputShape = db.query<{ value: string }>(`SELECT value FROM ${tableNames.metadataTable} WHERE key = 'input_schema_shape'`).get()?.value;
                const storedOutputShape = db.query<{ value: string }>(`SELECT value FROM ${tableNames.metadataTable} WHERE key = 'output_schema_shape'`).get()?.value;

                if (storedInputShape === undefined || storedOutputShape === undefined) {
                    console.log(`[Database ${baseName}] Storing initial schema shapes.`);
                    db.query(`INSERT OR REPLACE INTO ${tableNames.metadataTable} (key, value) VALUES (?, ?)`).run('input_schema_shape', currentInputShapeStr);
                    db.query(`INSERT OR REPLACE INTO ${tableNames.metadataTable} (key, value) VALUES (?, ?)`).run('output_schema_shape', currentOutputShapeStr);
                } else {
                    if (storedInputShape !== currentInputShapeStr) throw new Error(`[Database ${baseName}] Input schema mismatch detected. Expected: ${storedInputShape}, Got: ${currentInputShapeStr}`);
                    if (storedOutputShape !== currentOutputShapeStr) throw new Error(`[Database ${baseName}] Output schema mismatch detected. Expected: ${storedOutputShape}, Got: ${currentOutputShapeStr}`);
                    console.log(`[Database ${baseName}] Schema shapes verified.`);
                }

                // Ensure columns exist for current schemas (handles schema evolution)
                ensureTableColumns(db, tableNames.inputTable, inputSchema, false);
                ensureTableColumns(db, tableNames.outputTable, outputSchema, true);

                // Create indexes and triggers
                createAutomaticIndexes(db, tableNames.inputTable, inputSchema).catch(e => console.error(`[DB Indexing ${baseName}] Error creating input indexes:`, e));
                createAutomaticIndexes(db, tableNames.outputTable, outputSchema).catch(e => console.error(`[DB Indexing ${baseName}] Error creating output indexes:`, e));
                setupTriggers();
            })();
            initialized = true;
            console.log(`[Database ${baseName}] Initialized.`);
        },

        // Insert logic remains the same, relies on parse, processObjectForStorage etc.
        async insert(input: z.infer<I>, output?: z.infer<O>): Promise<{ id: string, output_ignored?: boolean }> {
            if (!initialized) await coreApi.init();
            // Zod v4: .parse() behavior remains the same for basic validation
            const validatedInput = inputSchema.parse(input);
            const id = generateRecordId(validatedInput, inputSchema);

            let validatedOutput: z.infer<O> | undefined = undefined;
            if (output !== undefined) { // Check specifically for undefined, as null might be valid
                 validatedOutput = outputSchema.parse(output);
            }

            let output_ignored = false;

            db.transaction(() => {
                const processedInput = processObjectForStorage(validatedInput, inputSchema);
                const inputColumns = Object.keys(processedInput);
                if (inputColumns.length === 0 && Object.keys(validatedInput).length > 0) {
                     console.warn(`[Database ${baseName}] Input object for ID ${id} resulted in zero columns for storage.`);
                 }
                const inputPlaceholders = inputColumns.map(() => "?").join(", ");
                // Insert input (ignore if ID exists)
                db.query(`INSERT INTO ${tableNames.inputTable} (id${inputColumns.length > 0 ? ", " + inputColumns.join(", ") : ""}) VALUES (?${inputPlaceholders.length > 0 ? ", " + inputPlaceholders : ""}) ON CONFLICT(id) DO NOTHING`)
                  .run(id, ...Object.values(processedInput));


                if (validatedOutput) {
                    const processedOutput = processObjectForStorage(validatedOutput, outputSchema);
                    const outputColumns = Object.keys(processedOutput);

                    if (outputColumns.length > 0) {
                         const outputPlaceholders = outputColumns.map(() => "?").join(", ");
                         // Insert output (ignore if ID exists)
                         const result = db.query(`INSERT OR IGNORE INTO ${tableNames.outputTable} (id, ${outputColumns.join(", ")}) VALUES (?, ${outputPlaceholders})`)
                           .run(id, ...Object.values(processedOutput));
                         if (result.changes === 0) output_ignored = true; // Output was ignored (likely already existed)
                    } else {
                         // If the validated output results in no columns, ensure no orphaned empty output row exists.
                         // However, inserting an empty row might be desired if the output schema *only* contains optional fields
                         // and they are all undefined. The current `INSERT OR IGNORE` handles the "already exists" case.
                         // If the goal is to ensure *some* output row exists even if empty, a different strategy is needed.
                         // Current strategy: Only insert if there are columns to insert.
                         console.warn(`[Database ${baseName}] Output object for ID ${id} resulted in zero columns for storage.`);
                    }
                }
                // If output was explicitly provided as 'undefined' but not null, we don't insert/touch the output table here.
                // If output was explicitly provided as 'null', outputSchema.parse(null) would likely throw unless schema allows null.
            })();

            if (output_ignored) console.log(`[Database ${baseName}] Output ignored for ID ${id} (already exists or no columns to insert).`);
            return { id, output_ignored };
        },

       async upsert(input: z.infer<I>, output: z.infer<O>): Promise<{ id: string }> {
            if (!initialized) await coreApi.init();
            const validatedInput = inputSchema.parse(input);
            const id = generateRecordId(validatedInput, inputSchema);
            const validatedOutput = outputSchema.parse(output); // Ensure output is valid

            db.transaction(() => {
                // Insert or Ignore Input
                const processedInput = processObjectForStorage(validatedInput, inputSchema);
                const inputColumns = Object.keys(processedInput);
                const inputPlaceholders = inputColumns.map(() => "?").join(", ");
                db.query(`INSERT INTO ${tableNames.inputTable} (id${inputColumns.length > 0 ? ", " + inputColumns.join(", ") : ""}) VALUES (?${inputPlaceholders.length > 0 ? ", " + inputPlaceholders : ""}) ON CONFLICT(id) DO NOTHING`)
                  .run(id, ...Object.values(processedInput));

                // Insert or Replace Output
                const processedOutput = processObjectForStorage(validatedOutput, outputSchema);
                const outputColumns = Object.keys(processedOutput);
                if (outputColumns.length > 0) {
                    const outputPlaceholders = outputColumns.map(() => "?").join(", ");
                    // Use INSERT OR REPLACE for the output table in upsert
                    db.query(`INSERT OR REPLACE INTO ${tableNames.outputTable} (id, ${outputColumns.join(", ")}) VALUES (?, ${outputPlaceholders})`)
                      .run(id, ...Object.values(processedOutput));
                } else {
                    // If the validated output has no columns, ensure any existing output for this ID is removed.
                    db.query(`DELETE FROM ${tableNames.outputTable} WHERE id = ?`).run(id);
                    console.warn(`[Database ${baseName} - Upsert] Output object for ID ${id} resulted in zero columns; existing output (if any) removed.`);
                }
            })();
            return { id };
        },


        // findRaw logic remains the same, relies on buildWhereClause, mapRowToRawRecord
        async findRaw(filter?): Promise<Array<RawDatabaseRecord>> {
            if (!initialized) await coreApi.init();
            const params: any[] = [];
            // Select explicitly named columns to avoid ambiguity if schemas share names (besides 'id')
            const inputCols = ['id', ...Object.keys(inputSchema.shape)].map(c => `i."${c}" AS "i_${c}"`).join(', ');
            const outputCols = Object.keys(outputSchema.shape).map(c => `o."${c}" AS "o_${c}"`).join(', '); // Exclude id from output select
            let sql = `SELECT ${inputCols}${outputCols ? ', ' + outputCols : ''} FROM ${tableNames.inputTable} i LEFT JOIN ${tableNames.outputTable} o ON i.id = o.id`;

            const whereClause = buildWhereClause(filter, params, 'i', 'o');
            sql += ` ${whereClause}`;

            // Add condition to ensure output exists if output filters are present
             if (filter?.output && Object.keys(filter.output).length > 0) {
                 // Check if an output row exists using a subquery or ensuring o.id is not null
                 sql += (whereClause ? " AND" : " WHERE") + " o.id IS NOT NULL";
             }

            const results = db.query(sql).all(...params);

             // Adjust mapRowToRawRecord to handle prefixed columns
             const mapPrefixedRow = (row: any): RawDatabaseRecord | null => {
                 if (!row || typeof row.i_id !== 'string') return null;
                 const inputData: Record<string, any> = {};
                 const outputData: Record<string, any> = {};
                 let hasNonNullOutputDbValue = false;

                 for (const [key, value] of Object.entries(row)) {
                     if (key.startsWith('i_')) {
                         const originalKey = key.substring(2);
                         if (originalKey === 'id') continue; // Handled separately
                         if (inputSchema.shape[originalKey]) {
                             inputData[originalKey] = deserializeValue(value, inputSchema.shape[originalKey]);
                         }
                     } else if (key.startsWith('o_')) {
                         const originalKey = key.substring(2);
                         if (outputSchema.shape[originalKey]) {
                             outputData[originalKey] = deserializeValue(value, outputSchema.shape[originalKey]);
                              if (value !== null) { // Check the raw DB value was not null
                                  hasNonNullOutputDbValue = true;
                              }
                         }
                     }
                 }
                  return {
                      id: row.i_id,
                      input: inputData,
                      ...(hasNonNullOutputDbValue && { output: outputData }) // Include output only if some output field was present
                  };
             };


             return results.map(row => mapPrefixedRow(row)).filter(record => record !== null) as Array<RawDatabaseRecord>;
        },


        // findByIdRaw logic remains the same, relies on mapRowToRawRecord
        async findByIdRaw(id: string): Promise<RawDatabaseRecord | null> {
             if (!initialized) await coreApi.init();
             // Select explicitly named columns like in findRaw
            const inputCols = ['id', ...Object.keys(inputSchema.shape)].map(c => `i."${c}" AS "i_${c}"`).join(', ');
            const outputCols = Object.keys(outputSchema.shape).map(c => `o."${c}" AS "o_${c}"`).join(', ');
             const sql = `SELECT ${inputCols}${outputCols ? ', ' + outputCols : ''} FROM ${tableNames.inputTable} i LEFT JOIN ${tableNames.outputTable} o ON i.id = o.id WHERE i.id = ?`;
             const row = db.query(sql).get(id);

              // Adjust mapRowToRawRecord to handle prefixed columns (same as in findRaw)
             const mapPrefixedRow = (row: any): RawDatabaseRecord | null => {
                 if (!row || typeof row.i_id !== 'string') return null;
                 const inputData: Record<string, any> = {};
                 const outputData: Record<string, any> = {};
                 let hasNonNullOutputDbValue = false;

                 for (const [key, value] of Object.entries(row)) {
                     if (key.startsWith('i_')) {
                         const originalKey = key.substring(2);
                          if (originalKey === 'id') continue;
                         if (inputSchema.shape[originalKey]) {
                             inputData[originalKey] = deserializeValue(value, inputSchema.shape[originalKey]);
                         }
                     } else if (key.startsWith('o_')) {
                         const originalKey = key.substring(2);
                         if (outputSchema.shape[originalKey]) {
                             outputData[originalKey] = deserializeValue(value, outputSchema.shape[originalKey]);
                             if (value !== null) { hasNonNullOutputDbValue = true;}
                         }
                     }
                 }
                  return {
                      id: row.i_id,
                      input: inputData,
                      ...(hasNonNullOutputDbValue && { output: outputData })
                  };
             };

             return mapPrefixedRow(row);
        },

        // updateOutput logic relies on parse, partial, processObjectForStorage - should be fine
        async updateOutput(input: z.infer<I>, output: Partial<z.infer<O>>): Promise<{ id: string }> {
            if (!initialized) await coreApi.init();
            const validatedInput = inputSchema.parse(input);
            const id = generateRecordId(validatedInput, inputSchema);
            // Zod v4: .partial() still works
            const validatedOutputUpdate = outputSchema.partial().parse(output);
            if (Object.keys(validatedOutputUpdate).length === 0) {
                console.log(`[Database ${baseName}] updateOutput called with empty output object for ID ${id}. No changes made.`);
                return { id }; // No update to perform
            }

            // Check if input exists
            const inputExists = db.query(`SELECT 1 FROM ${tableNames.inputTable} WHERE id = ?`).get(id);
            if (!inputExists) {
                throw new Error(`[Database ${baseName}] UpdateOutput failed: Input ID ${id} not found.`);
            }

            // Check if output already exists - updateOutput is specifically for *adding* output once.
            const existingOutputRow = db.query(`SELECT 1 FROM ${tableNames.outputTable} WHERE id = ?`).get(id);
            if (existingOutputRow) {
                // If output exists, this method should fail as per its contract (write-once for output)
                 // If you need update/overwrite semantics, use `upsert` instead.
                throw new Error(`[Database ${baseName}] UpdateOutput failed: Output already exists for ID ${id}. Cannot overwrite using updateOutput. Use upsert if overwrite is intended.`);
            }

            // Since output doesn't exist, we treat the partial update as the *initial* insert data.
            // We need to ensure the *combination* would be valid if merged with defaults,
            // but since we are inserting fresh, we just need to parse the partial data *as if*
            // it were the full object, letting Zod apply defaults for missing fields if defined.
            // A simple `outputSchema.parse(validatedOutputUpdate)` might fail if required fields are missing.
            // Better: apply the update to a default object, then parse.
            let fullOutputData: z.infer<O>;
             try {
                 // Attempt to parse the partial data directly, Zod v4 might handle defaults better here.
                 // If the output schema has defaults for fields missing in the partial, this *might* work.
                 // However, for required fields without defaults, this will fail.
                 // A more robust approach might involve merging with defaults manually before parsing,
                 // but let's try the simpler parse first. If it fails, it indicates required data is missing.
                 fullOutputData = outputSchema.parse(validatedOutputUpdate);
             } catch (parseError: any) {
                 console.error(`[Database ${baseName}] Failed to parse partial output for ID ${id} into full schema. Ensure all required fields (or fields with defaults) are covered.`, parseError.errors);
                 throw new Error(`[Database ${baseName}] UpdateOutput failed for ID ${id}: Provided partial output is insufficient to form a valid output record according to the schema. ${parseError.message}`);
             }


            const processedOutput = processObjectForStorage(fullOutputData, outputSchema);
            const columns = Object.keys(processedOutput);

            if (columns.length > 0) {
                const placeholders = columns.map(() => "?").join(", ");
                 // Use INSERT OR IGNORE - though we already checked existence, this adds safety against race conditions.
                db.query(`INSERT OR IGNORE INTO ${tableNames.outputTable} (id, ${columns.join(", ")}) VALUES (?, ${placeholders})`)
                  .run(id, ...Object.values(processedOutput));
            } else {
                console.warn(`[Database ${baseName}] updateOutput for ID ${id} resulted in zero columns after processing. No output row inserted.`);
                 // Decide if an empty output row should be inserted here. Current logic: No.
            }
            return { id };
        },

        // delete logic remains the same, relies on parse
        async delete(input: z.infer<I>): Promise<{ id: string }> {
            if (!initialized) await coreApi.init();
            const validatedInput = inputSchema.parse(input);
            const id = generateRecordId(validatedInput, inputSchema);
            const result = db.query(`DELETE FROM ${tableNames.inputTable} WHERE id = ?`).run(id);
            // Foreign key CASCADE should handle deleting the corresponding output row automatically.
            if (result.changes === 0) {
                throw new Error(`[Database ${baseName}] Delete failed: Record ID ${id} not found.`);
            }
            return { id };
        },

        // query logic remains the same
        async query<T = any>(sql: string, ...params: any[]): Promise<T[]> {
            if (!initialized) await coreApi.init(); // Ensure DB is ready
            try {
                // Bun's API might return results directly or need .all()
                const statement = db.prepare(sql);
                 // Check if statement expects parameters before trying to bind
                 if (statement.paramsCount > 0) {
                     return statement.all(...params) as T[];
                 } else {
                     return statement.all() as T[];
                 }
            } catch (error: any) {
                console.error(`[DB Query ${baseName}] Error executing SQL: "${sql}" with params: ${JSON.stringify(params)}`, error?.message);
                throw error; // Re-throw the original error
            }
        },

        // Schema getters remain the same
        getInputSchema: () => inputSchema,
        getOutputSchema: () => outputSchema,
        getName: () => baseName,

        // close logic remains the same
        close(): void {
            if (db && !db.closed) {
                try { db.close(); } catch (e: any) { console.error(`[Database ${baseName}] Error closing DB:`, e?.message); }
            }
            initialized = false;
            console.log(`[Database ${baseName}] Closed.`);
        },

        // Internal methods remain the same
        _internal: {
            generateId: (input: z.infer<I>) => generateRecordId(input, inputSchema),
            getDbInstance: () => db,
            getTableNames: () => tableNames,
            isInitialized: () => initialized,
        }
    };
    return coreApi;
}