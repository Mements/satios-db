import { Database, Statement } from "bun:sqlite";
import { z } from "zod";
import path from "path";
import fs from "node:fs";
import { createHash } from "node:crypto";

// Define or import RESERVED_SQLITE_WORDS robustly
const RESERVED_SQLITE_WORDS = new Set(["ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE", "AND", "AS", "ASC", "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT", "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DO", "DROP", "EACH", "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUDE", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FILTER", "FIRST", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL", "GENERATED", "GLOB", "GROUP", "GROUPS", "HAVING", "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LAST", "LEFT", "LIKE", "LIMIT", "MATCH", "MATERIALIZED", "NATURAL", "NO", "NOT", "NOTHING", "NOTNULL", "NULL", "NULLS", "OF", "OFFSET", "ON", "OR", "ORDER", "OTHERS", "OUTER", "OVER", "PARTITION", "PLAN", "PRAGMA", "PRECEDING", "PRIMARY", "QUERY", "RAISE", "RANGE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RIGHT", "ROLLBACK", "ROW", "ROWS", "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION", "TRIGGER", "UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE", "WINDOW", "WITH", "WITHOUT"]);

// --- Base Types ---
/**
 * Represents a raw record retrieved from the database, before Zod parsing.
 * Used for performance optimization on reads.
 */
export type RawDatabaseRecord = {
    id: string;
    input: Record<string, any>; // Deserialized input data
    output?: Record<string, any>; // Deserialized output data (if present)
};

/** Defines a range filter [min, max] for queries. */
export type RangeFilter<T> = [T, T];

/** Defines filter criteria for input/output schemas, allowing partials and range filters. */
export type Filter<T extends z.ZodObject<any>> = Partial<z.infer<T> & {
    [K in keyof z.infer<T>]?: z.infer<T>[K] | RangeFilter<z.infer<T>[K]>
}>;

/** Holds the names of the tables used by the database instance. */
export interface TableNames {
    inputTable: string;
    outputTable: string;
    changesTable: string;
    metadataTable: string;
}

// --- Zod Type Guards ---
// These helpers check the type of a Zod schema definition.
function isZodString(value: z.ZodTypeAny): value is z.ZodString { return value._def.typeName === "ZodString"; }
function isZodNumber(value: z.ZodTypeAny): value is z.ZodNumber { return value._def.typeName === "ZodNumber"; }
function isZodBoolean(value: z.ZodTypeAny): value is z.ZodBoolean { return value._def.typeName === "ZodBoolean"; }
function isZodEnum(value: z.ZodTypeAny): value is z.ZodEnum<any> { return value._def.typeName === "ZodEnum"; }
function isZodDate(value: z.ZodTypeAny): value is z.ZodDate { return value._def.typeName === "ZodDate"; }
function isZodArray(value: z.ZodTypeAny): value is z.ZodArray<any> { return value._def.typeName === "ZodArray"; }
function isZodObject(value: z.ZodTypeAny): value is z.ZodObject<any> { return value._def.typeName === "ZodObject"; }
function isZodNullable(value: z.ZodTypeAny): value is z.ZodNullable<any> { return value._def.typeName === "ZodNullable"; }
function isZodOptional(value: z.ZodTypeAny): value is z.ZodOptional<any> { return value._def.typeName === "ZodOptional"; }
function isZodDefault(value: z.ZodTypeAny): value is z.ZodDefault<any> { return value._def.typeName === "ZodDefault"; }
// --- End Zod Type Guards ---

// --- Shared Utilities ---

/**
 * Generates a deterministic SHA256 hash (substring) for a given input object.
 * Ensures consistent hashing by sorting object keys before stringification.
 * @param input The input object.
 * @param inputSchema The Zod schema for the input (used for type safety hint).
 * @returns A 16-character hex string ID.
 */
function generateRecordId<I extends z.ZodObject<any>>(input: z.infer<I>, inputSchema: I): string {
     // inputSchema.parse(input); // Optional: uncomment for extra safety check before hashing
     const orderedInput = Object.keys(input)
         .sort()
         .reduce((obj, key) => {
             let value = input[key];
             // Basic stable stringify for hashing consistency
             if (value !== null && typeof value === 'object') {
                  try {
                      value = JSON.stringify(value, (k, v) => {
                          if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
                              // Sort keys of nested objects
                              return Object.keys(v).sort().reduce((sortedObj, innerKey) => {
                                  sortedObj[innerKey] = v[innerKey]; return sortedObj;
                              }, {});
                          } return v;
                      });
                  } catch { value = JSON.stringify(value); } // Fallback stringify
             } else { value = JSON.stringify(value); } // Stringify primitives
             obj[key] = value;
             return obj;
         }, {});
     // Create hash and take the first 16 characters
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

/**
 * Creates and manages a connection to a Sati database instance.
 * Handles table creation, schema validation, indexing, triggers, and provides core CRUD methods.
 *
 * @param dbPath Path to the SQLite database file.
 * @param inputSchema Zod schema for the input data.
 * @param outputSchema Zod schema for the output data.
 * @returns A SatiDbCore object for interacting with the database.
 */
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

    // Create DB connection only once per instance
    const db = new Database(dbPath);
    try { db.exec("PRAGMA journal_mode = WAL;"); } catch (e) { console.warn(`[Database ${baseName}] Could not enable WAL mode:`, e.message); }
    let initialized = false;

    // --- Internal Utility Functions (Scoped to useDatabase) ---

    function getSqliteType(zodType: z.ZodTypeAny): string | null {
        const unwrapped = zodType.unwrap ? zodType.unwrap() : zodType;
        if (isZodString(unwrapped) || isZodEnum(unwrapped) || isZodArray(unwrapped) || isZodObject(unwrapped) || isZodDate(unwrapped)) return "TEXT";
        if (isZodNumber(unwrapped)) return "NUMERIC";
        if (isZodBoolean(unwrapped)) return "INTEGER";
        if (isZodDefault(zodType)) return getSqliteType(zodType._def.innerType);
        console.warn(`[Database Schema ${baseName}] Unsupported Zod type: ${zodType.constructor.name}`);
        return "TEXT";
    }

    function getDefaultValueClause(zodType: z.ZodTypeAny): string {
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

    function createTableSql(schema: z.ZodObject<any>, tableName: string, isOutputTable: boolean = false): string {
        const columns = ["id TEXT PRIMARY KEY"];
        for (const [key, valueDef] of Object.entries(schema.shape)) {
            let columnType = getSqliteType(valueDef);
            if (!columnType) continue;
            const isOptional = valueDef.isOptional() || valueDef.isNullable();
            const defaultClause = getDefaultValueClause(valueDef);
            const nullClause = (isOutputTable || (isOptional && !isZodDefault(valueDef))) ? " NULL" : (defaultClause ? "" : " NOT NULL");
            columns.push(`${key} ${columnType}${nullClause}${defaultClause}`);
        }
        const fkConstraint = isOutputTable ? `, FOREIGN KEY(id) REFERENCES ${tableNames.inputTable}(id) ON DELETE CASCADE` : "";
        return `CREATE TABLE IF NOT EXISTS ${tableName} (${columns.join(", ")}${fkConstraint})`;
    }

    async function createAutomaticIndexes(dbInstance: Database, tblName: string, schema: z.ZodObject<any>): Promise<void> {
        // Only attempt if DB is open
        if (!dbInstance || dbInstance.closed) return;
        try {
            const tableInfo: { name: string }[] = dbInstance.query(`PRAGMA table_info(${tblName})`).all() as any;
            for (const column of tableInfo) {
                if (column.name === "id" || !schema.shape[column.name]) continue;
                const zodType = schema.shape[column.name];
                const sqlType = getSqliteType(zodType);
                if (sqlType === "NUMERIC" || sqlType === "INTEGER" || isZodEnum(zodType) || isZodDate(zodType)) {
                    const indexName = `idx_${tblName}_${column.name}`;
                    try { dbInstance.query(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${tblName}(${column.name})`).run(); }
                    catch (error) { console.warn(`[DB Indexing ${baseName}] Failed index ${indexName}:`, error); }
                }
            }
        } catch (error) { if (!error.message.includes("no such table")) { console.error(`[DB Indexing ${baseName}] Error table info ${tblName}:`, error); } }
    }

    function processValueForStorage(value: any, zodType: z.ZodTypeAny): any {
        if (value === undefined || value === null) return null;
        const unwrapped = zodType.unwrap ? zodType.unwrap() : zodType;
        if (value instanceof Date) return value.toISOString();
        if (isZodBoolean(unwrapped)) return value ? 1 : 0;
        if (isZodArray(unwrapped) || isZodObject(unwrapped)) return JSON.stringify(value);
        if (isZodNumber(unwrapped) && typeof value === 'number' && !Number.isFinite(value)) { return null; }
        return value;
    }

    function processObjectForStorage(data: Record<string, any>, schema: z.ZodObject<any>): Record<string, any> {
        const result: Record<string, any> = {};
        for (const [key, value] of Object.entries(data)) {
            if (schema.shape[key]) { result[key] = processValueForStorage(value, schema.shape[key]); }
        } return result;
    }

    function deserializeValue(value: any, zodType: z.ZodTypeAny): any {
        if (value === null) return null;
        const unwrapped = zodType.unwrap ? zodType.unwrap() : zodType;
        try {
            if (isZodDate(unwrapped)) return new Date(value);
            if (isZodBoolean(unwrapped)) return Boolean(Number(value));
            if (isZodArray(unwrapped) || isZodObject(unwrapped)) return typeof value === 'string' ? JSON.parse(value) : value;
            if (isZodNumber(unwrapped)) return Number(value);
            if (isZodString(unwrapped) || isZodEnum(unwrapped)) return String(value);
        } catch (e) { console.error(`[DB Deserialization ${baseName}] Error value "${value}" type ${unwrapped?._def?.typeName}:`, e); return undefined; }
        return value;
    }

    function mapRowToRawRecord(row: any): RawDatabaseRecord | null {
        if (!row || typeof row.id !== 'string') return null;
        const rawInputData: Record<string, any> = {};
        const rawOutputData: Record<string, any> = {};
        let hasOutputField = false;
        for (const [key, dbValue] of Object.entries(row)) {
            if (key === 'id') continue;
            if (key in inputSchema.shape) {
                rawInputData[key] = deserializeValue(dbValue, inputSchema.shape[key]);
            } else if (key in outputSchema.shape) {
                const deserialized = deserializeValue(dbValue, outputSchema.shape[key]);
                if (deserialized !== null) { hasOutputField = true; }
                rawOutputData[key] = deserialized;
            }
        }
        return { id: row.id, input: rawInputData, ...(hasOutputField && { output: rawOutputData }) };
    }

    function buildWhereClause(filter: { input?: Filter<I>, output?: Filter<O> } | undefined, params: any[], inputAlias: string = 'i', outputAlias: string = 'o'): string {
        let clauses: string[] = [];
        const addFilters = (dataFilter: Record<string, any> | undefined, schema: z.ZodObject<any>, alias: string) => {
            if (!dataFilter) return;
            for (let [key, value] of Object.entries(dataFilter)) {
                if (value === undefined || !schema.shape[key]) continue;
                const zodType = schema.shape[key];
                if (Array.isArray(value) && value.length === 2 && !isZodArray(zodType)) {
                    const [min, max] = value;
                    clauses.push(`${alias}.${key} BETWEEN ? AND ?`);
                    params.push(processValueForStorage(min, zodType), processValueForStorage(max, zodType));
                } else {
                    clauses.push(`${alias}.${key} = ?`);
                    params.push(processValueForStorage(value, zodType));
                }
            }
        };
        addFilters(filter?.input, inputSchema, inputAlias);
        addFilters(filter?.output, outputSchema, outputAlias);
        return clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "";
    }

    function ensureTableColumns(dbInstance: Database, tableName: string, schema: z.ZodObject<any>, isOutputTable: boolean = false) {
        // Only run if DB is open
        if (!dbInstance || dbInstance.closed) return;
        try {
            const tableInfo: { name: string }[] = dbInstance.query(`PRAGMA table_info(${tableName})`).all() as any;
            const existingColumns = new Set(tableInfo.map(col => col.name));
            for (const [key, valueDef] of Object.entries(schema.shape)) {
                if (key === 'id') continue;
                if (!existingColumns.has(key)) {
                    let columnType = getSqliteType(valueDef);
                    if (columnType) {
                        const defaultClause = getDefaultValueClause(valueDef);
                        const isOptional = valueDef.isOptional() || valueDef.isNullable();
                        const nullClause = (isOutputTable || (isOptional && !isZodDefault(valueDef))) ? " NULL" : (defaultClause ? "" : " NOT NULL");
                        dbInstance.query(`ALTER TABLE ${tableName} ADD COLUMN ${key} ${columnType}${nullClause}${defaultClause}`).run();
                        console.log(`[DB Schema ${baseName}] Added column ${key} to ${tableName}`);
                    } else { console.warn(`[DB Schema ${baseName}] No SQLite type for ${key}`); }
                }
            }
        } catch (error) { if (!error.message.includes("no such table")) { console.error(`[DB Schema ${baseName}] Error ensuring columns ${tableName}:`, error); throw error; } }
    }

    // Setup triggers required by the agent layer to detect changes
    const setupTriggers = () => {
        if (!db || db.closed) return;
        db.transaction(() => {
            db.query(`CREATE TABLE IF NOT EXISTS ${tableNames.changesTable} (change_id INTEGER PRIMARY KEY AUTOINCREMENT, record_id TEXT NOT NULL, change_type TEXT NOT NULL, table_name TEXT NOT NULL, changed_at INTEGER NOT NULL DEFAULT (unixepoch()))`).run();
            db.query(`DROP TRIGGER IF EXISTS ${tableNames.inputTable}_insert_trigger`).run();
            db.query(`CREATE TRIGGER ${tableNames.inputTable}_insert_trigger AFTER INSERT ON ${tableNames.inputTable} BEGIN INSERT INTO ${tableNames.changesTable} (record_id, change_type, table_name) VALUES (NEW.id, 'input_added', 'input'); END;`).run();
            db.query(`DROP TRIGGER IF EXISTS ${tableNames.inputTable}_delete_trigger`).run();
            db.query(`CREATE TRIGGER ${tableNames.inputTable}_delete_trigger AFTER DELETE ON ${tableNames.inputTable} BEGIN INSERT INTO ${tableNames.changesTable} (record_id, change_type, table_name) VALUES (OLD.id, 'record_deleted', 'input'); END;`).run();
            db.query(`DROP TRIGGER IF EXISTS ${tableNames.outputTable}_insert_trigger`).run();
            db.query(`CREATE TRIGGER ${tableNames.outputTable}_insert_trigger AFTER INSERT ON ${tableNames.outputTable} BEGIN INSERT INTO ${tableNames.changesTable} (record_id, change_type, table_name) VALUES (NEW.id, 'output_added', 'output'); END;`).run();
            db.query(`DROP TRIGGER IF EXISTS ${tableNames.outputTable}_update_trigger`).run();
            db.query(`CREATE TRIGGER ${tableNames.outputTable}_update_trigger AFTER UPDATE ON ${tableNames.outputTable} BEGIN INSERT INTO ${tableNames.changesTable} (record_id, change_type, table_name) VALUES (NEW.id, 'record_updated', 'output'); END;`).run();
        })();
    };

    // Stringify schema shape for storage/comparison
    const stringifySchemaShape = (schema: z.ZodObject<any>): string => {
        const shape = schema.shape;
        const sortedKeys = Object.keys(shape).sort();
        const shapeRepresentation: Record<string, string> = {};
        for (const key of sortedKeys) { shapeRepresentation[key] = shape[key]._def.typeName; }
        return JSON.stringify(shapeRepresentation);
    };

    // --- Public API Implementation ---
    const coreApi: SatiDbCore<I, O> = {
        async init(): Promise<void> {
            if (initialized || !db || db.closed) return; // Prevent re-init or init on closed DB
            db.transaction(() => {
                db.query(createTableSql(inputSchema, tableNames.inputTable, false)).run();
                db.query(createTableSql(outputSchema, tableNames.outputTable, true)).run();
                db.query(`CREATE TABLE IF NOT EXISTS ${tableNames.metadataTable} (key TEXT PRIMARY KEY, value TEXT NOT NULL)`).run();

                const currentInputShapeStr = stringifySchemaShape(inputSchema);
                const currentOutputShapeStr = stringifySchemaShape(outputSchema);
                const storedInputShape = db.query<{ value: string }>(`SELECT value FROM ${tableNames.metadataTable} WHERE key = 'input_schema_shape'`).get()?.value;
                const storedOutputShape = db.query<{ value: string }>(`SELECT value FROM ${tableNames.metadataTable} WHERE key = 'output_schema_shape'`).get()?.value;

                if (storedInputShape === undefined || storedOutputShape === undefined) {
                    console.log(`[Database ${baseName}] Storing schema shapes.`);
                    db.query(`INSERT OR REPLACE INTO ${tableNames.metadataTable} (key, value) VALUES (?, ?)`).run('input_schema_shape', currentInputShapeStr);
                    db.query(`INSERT OR REPLACE INTO ${tableNames.metadataTable} (key, value) VALUES (?, ?)`).run('output_schema_shape', currentOutputShapeStr);
                } else {
                    if (storedInputShape !== currentInputShapeStr) throw new Error(`[Database ${baseName}] Input schema mismatch.`);
                    if (storedOutputShape !== currentOutputShapeStr) throw new Error(`[Database ${baseName}] Output schema mismatch.`);
                    console.log(`[Database ${baseName}] Schema verified.`);
                }

                ensureTableColumns(db, tableNames.inputTable, inputSchema, false);
                ensureTableColumns(db, tableNames.outputTable, outputSchema, true);
                createAutomaticIndexes(db, tableNames.inputTable, inputSchema).catch(e => console.error(`Index error: ${e}`));
                setupTriggers(); // Setup triggers needed for agent layer
            })();
            initialized = true;
            console.log(`[Database ${baseName}] Initialized.`);
        },

        async insert(input: z.infer<I>, output?: z.infer<O>): Promise<{ id: string, output_ignored?: boolean }> {
            console.log("output", output)
            console.log("input", input)
            if (!initialized) await coreApi.init();
            const validatedInput = inputSchema.parse(input);
            const id = generateRecordId(validatedInput, inputSchema);
            console.log("id", id)
            let validatedOutput: z.infer<O> | undefined = undefined;
            if (output) validatedOutput = outputSchema.parse(output);
            console.log("validatedOutput", validatedOutput)
            let output_ignored = false;

            db.transaction(() => {
                const processedInput = processObjectForStorage(validatedInput, inputSchema);
                console.log("processedInput", processedInput)
                const inputColumns = Object.keys(processedInput);
                const inputPlaceholders = inputColumns.map(() => "?").join(", ");
                const res = db.query(`INSERT INTO ${tableNames.inputTable} (id, ${inputColumns.join(", ")}) VALUES (?, ${inputPlaceholders}) ON CONFLICT(id) DO NOTHING`)
                  .run(id, ...Object.values(processedInput));
                  console.log("res", res)

                if (validatedOutput) {
                    const processedOutput = processObjectForStorage(validatedOutput, outputSchema);
                    const outputColumns = Object.keys(processedOutput);
                    if (outputColumns.length > 0) {
                        const outputPlaceholders = outputColumns.map(() => "?").join(", ");
                        const result = db.query(`INSERT OR IGNORE INTO ${tableNames.outputTable} (id, ${outputColumns.join(", ")}) VALUES (?, ${outputPlaceholders})`)
                                         .run(id, ...Object.values(processedOutput));
                        if (result.changes === 0) output_ignored = true;
                    } else { db.query(`DELETE FROM ${tableNames.outputTable} WHERE id = ?`).run(id); }
                }
            })();
            if (output_ignored) console.log(`[Database ${baseName}] Output ignored for ID ${id} (already exists).`);
            return { id, output_ignored };
        },

        async findRaw(filter?): Promise<Array<RawDatabaseRecord>> {
             if (!initialized) await coreApi.init();
             const params: any[] = [];
             let sql = `SELECT i.*, o.* FROM ${tableNames.inputTable} i LEFT JOIN ${tableNames.outputTable} o ON i.id = o.id`;
             const whereClause = buildWhereClause(filter, params, 'i', 'o');
             sql += ` ${whereClause}`;
             if (filter?.output && Object.keys(filter.output).length > 0) {
                 sql += (whereClause ? " AND" : " WHERE") + " o.id IS NOT NULL";
             }
             const results = db.query(sql).all(...params);
             return results.map(row => mapRowToRawRecord(row)).filter(record => record !== null) as Array<RawDatabaseRecord>;
        },

        async findByIdRaw(id: string): Promise<RawDatabaseRecord | null> {
            if (!initialized) await coreApi.init();
            const row = db.query(`SELECT  i.*, o.*, i.id as id FROM ${tableNames.inputTable} i LEFT JOIN ${tableNames.outputTable} o ON i.id = o.id WHERE i.id = ?`).get(id);
            console.log("row", row)
            return mapRowToRawRecord(row);
        },

        async updateOutput(input: z.infer<I>, output: Partial<z.infer<O>>): Promise<{ id: string }> {
             if (!initialized) await coreApi.init();
             const validatedInput = inputSchema.parse(input);
             const id = generateRecordId(validatedInput, inputSchema);
             const validatedOutputUpdate = outputSchema.partial().parse(output);
             if (Object.keys(validatedOutputUpdate).length === 0) return { id };

             const inputExists = db.query(`SELECT 1 FROM ${tableNames.inputTable} WHERE id = ?`).get(id);
             if (!inputExists) throw new Error(`UpdateOutput failed: Input ID ${id} not found.`);

             const existingOutputRow = db.query(`SELECT 1 FROM ${tableNames.outputTable} WHERE id = ?`).get(id);
             if (existingOutputRow) throw new Error(`UpdateOutput failed: Output already exists for ID ${id}. Cannot overwrite.`);

             // Fetch existing output schema fields to merge (necessary for full validation)
             // This seems complex if we only have partial output. Let's simplify:
             // Since output doesn't exist, we treat the partial update as the full intended output.
             const validatedFullOutput = outputSchema.parse(validatedOutputUpdate); // Parse the partial as if it's the full required output
             const processedOutput = processObjectForStorage(validatedFullOutput, outputSchema);
             const columns = Object.keys(processedOutput);

             if (columns.length > 0) {
                 const placeholders = columns.map(() => "?").join(", ");
                 db.query(`INSERT OR IGNORE INTO ${tableNames.outputTable} (id, ${columns.join(", ")}) VALUES (?, ${placeholders})`)
                   .run(id, ...Object.values(processedOutput));
             }
             return { id };
        },

        async delete(input: z.infer<I>): Promise<{ id: string }> {
            if (!initialized) await coreApi.init();
            const validatedInput = inputSchema.parse(input);
            const id = generateRecordId(validatedInput, inputSchema);
            const result = db.query(`DELETE FROM ${tableNames.inputTable} WHERE id = ?`).run(id);
            if (result.changes === 0) throw new Error(`Delete failed: Record ID ${id} not found.`);
            return { id };
        },

        async query<T = any>(sql: string, ...params: any[]): Promise<T[]> {
             if (!initialized) await coreApi.init();
             try { return db.prepare(sql).all(...params) as T[]; }
             catch (error) { console.error(`[DB Query ${baseName}] Error: "${sql}"`, error); throw error; }
        },

        getInputSchema: () => inputSchema,
        getOutputSchema: () => outputSchema,
        getName: () => baseName,
        close(): void {
             if (db && !db.closed) {
                  try { db.close(); } catch (e) { console.error(`[Database ${baseName}] Error closing DB:`, e); }
             }
             initialized = false; // Reset initialized state on close
             console.log(`[Database ${baseName}] Closed.`);
        },
        // Expose internal methods/properties via a nested object
        _internal: {
            generateId: (input: z.infer<I>) => generateRecordId(input, inputSchema),
            getDbInstance: () => db,
            getTableNames: () => tableNames,
            isInitialized: () => initialized,
        }
    };
    return coreApi;
}
