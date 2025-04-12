import { Database, Statement } from "bun:sqlite";
import { z } from "zod";
import path from "path";
import fs from "node:fs";
import { createHash } from "node:crypto";
import { EventEmitter } from "events";

// Define or import RESERVED_SQLITE_WORDS robustly
const RESERVED_SQLITE_WORDS = new Set(["ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE", "AND", "AS", "ASC", "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT", "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DO", "DROP", "EACH", "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUDE", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FILTER", "FIRST", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL", "GENERATED", "GLOB", "GROUP", "GROUPS", "HAVING", "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LAST", "LEFT", "LIKE", "LIMIT", "MATCH", "MATERIALIZED", "NATURAL", "NO", "NOT", "NOTHING", "NOTNULL", "NULL", "NULLS", "OF", "OFFSET", "ON", "OR", "ORDER", "OTHERS", "OUTER", "OVER", "PARTITION", "PLAN", "PRAGMA", "PRECEDING", "PRIMARY", "QUERY", "RAISE", "RANGE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RIGHT", "ROLLBACK", "ROW", "ROWS", "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION", "TRIGGER", "UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE", "WINDOW", "WITH", "WITHOUT"]);

// --- Base Database Interfaces ---
export interface DatabaseRecord<I, O> {
    id: string;
    input: I;
    output?: O;
}
// Raw record type returned before Zod parsing (for performance optimization)
export type RawDatabaseRecord = {
    id: string;
    input: Record<string, any>; // Deserialized input data
    output?: Record<string, any>; // Deserialized output data (if present)
};
// Type for raw event data
export interface RawChangeEvent {
    type: 'input_added' | 'output_added' | 'record_updated' | 'record_deleted';
    id: string;
    input?: Record<string, any>; // Raw input
    output?: Record<string, any>; // Raw output
}
// Type for parsed event data (emitted by listener)
export interface ParsedChangeEvent<I, O> {
    type: 'input_added' | 'output_added' | 'record_updated' | 'record_deleted';
    id: string;
    input?: I; // Parsed input
    output?: O; // Parsed output
}

export type RangeFilter<T> = [T, T];
export type Filter<T extends z.ZodObject<any>> = Partial<z.infer<T> & {
    [K in keyof z.infer<T>]?: z.infer<T>[K] | RangeFilter<z.infer<T>[K]>
}>;

// Interface for the core database operations (internal)
// Note: find/findById now return RawDatabaseRecord for performance
interface SatiDatabase<I extends z.ZodObject<any>, O extends z.ZodObject<any>> {
    init: () => Promise<void>;
    insert: (input: z.infer<I>, output?: z.infer<O>) => Promise<{ id: string, output_ignored?: boolean }>; // Indicate if output was ignored
    findRaw: (filter: { input?: Filter<I>, output?: Filter<O> }) => Promise<Array<RawDatabaseRecord>>; // Returns raw records
    findByIdRaw: (id: string) => Promise<RawDatabaseRecord | null>; // Returns raw record
    update: (input: z.infer<I>, output: Partial<z.infer<O>>) => Promise<{ id: string }>; // Throws if output exists
    delete: (input: z.infer<I>) => Promise<{ id: string }>;
    query: <T = any>(sql: string, ...params: any[]) => Promise<T[]>;
    // Listener emits ParsedChangeEvent after internal validation
    listen: (callback: (event: ParsedChangeEvent<z.infer<I>, z.infer<O>>) => void) => { stop: () => void };
    getInputSchema: () => I;
    getOutputSchema: () => O;
    getName: () => string;
    close: () => void;
    _generateId: (input: z.infer<I>) => string;
    _getDbInstance: () => Database;
}

// --- Zod Type Guards ---
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

// --- Shared ID Generation Logic ---
function generateRecordId<I extends z.ZodObject<any>>(input: z.infer<I>, inputSchema: I): string {
     // Ensure input is validated before hashing (caller responsibility, but good practice)
     // inputSchema.parse(input); // Optional: uncomment for extra safety here
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
                              }, {});
                          } return v;
                      });
                  } catch { value = JSON.stringify(value); }
             } else { value = JSON.stringify(value); }
             obj[key] = value;
             return obj;
         }, {});
     return createHash('sha256').update(JSON.stringify(orderedInput)).digest('hex').substring(0, 16);
}

// --- Utility to sanitize names ---
function sanitizeName(name: string): string {
    let sanitized = name.replace(/[^a-zA-Z0-9_]/g, "_");
    if (/^[0-9]/.test(sanitized)) sanitized = "t_" + sanitized;
    if (RESERVED_SQLITE_WORDS.has(sanitized.toUpperCase())) sanitized = "t_" + sanitized;
    return sanitized;
}

// --- Core Database Implementation (Internal) ---
function useDatabaseInternal<I extends z.ZodObject<any>, O extends z.ZodObject<any>>(
    dbPath: string,
    inputSchema: I,
    outputSchema: O
): SatiDatabase<I, O> {
    const baseName = path.basename(dbPath, path.extname(dbPath));
    const sanitizedName = sanitizeName(baseName);
    const inputTable = `input_${sanitizedName}`;
    const outputTable = `output_${sanitizedName}`;
    const changesTable = `_changes_${sanitizedName}`;
    const metadataTable = `_sati_metadata_${sanitizedName}`; // Unique metadata table per DB name

    if ('id' in inputSchema.shape || 'id' in outputSchema.shape) {
        throw new Error(`[Database Setup ${baseName}] Schemas cannot contain 'id'.`);
    }

    const dir = path.dirname(dbPath);
    try { fs.mkdirSync(dir, { recursive: true }); } catch (error: any) { if (error.code !== 'EEXIST') throw error; }

    const db = new Database(dbPath);
    try { db.exec("PRAGMA journal_mode = WAL;"); } catch (e) { console.warn(`[Database ${baseName}] Could not enable WAL mode:`, e.message); }
    let initialized = false;
    const eventEmitter = new EventEmitter();
    let listenerStopHandle: { stop: () => void } | null = null;
    let activeListenersCount = 0;
    let lastChangeId = 0;


    // --- Internal Utility Functions ---


    function getSqliteType(zodType: z.ZodTypeAny): string | null {
        const unwrapped = zodType.unwrap ? zodType.unwrap() : zodType;
        if (isZodString(unwrapped) || isZodEnum(unwrapped) || isZodArray(unwrapped) || isZodObject(unwrapped) || isZodDate(unwrapped)) return "TEXT"; // Dates as ISO TEXT
        if (isZodNumber(unwrapped)) return "NUMERIC";
        if (isZodBoolean(unwrapped)) return "INTEGER"; // Booleans as 0/1
        if (isZodDefault(zodType)) return getSqliteType(zodType._def.innerType);
        console.warn(`[Database Schema ${baseName}] Unsupported Zod type for SQLite mapping: ${zodType.constructor.name}`);
        return "TEXT"; // Default fallback
    }

     function getDefaultValueClause(zodType: z.ZodTypeAny): string {
         if (isZodDefault(zodType)) {
             const defaultValue = typeof zodType._def.defaultValue === 'function'
                 ? zodType._def.defaultValue()
                 : zodType._def.defaultValue;

             if (defaultValue === undefined || defaultValue === null) return "";

             let formattedDefault: string | number | null = null;
             if (typeof defaultValue === 'string') formattedDefault = `'${defaultValue.replace(/'/g, "''")}'`;
             else if (typeof defaultValue === 'number') formattedDefault = defaultValue;
             else if (typeof defaultValue === 'boolean') formattedDefault = defaultValue ? 1 : 0;
             else if (defaultValue instanceof Date) formattedDefault = `'${defaultValue.toISOString()}'`;
             // Avoid complex object defaults in SQL DDL

             if (formattedDefault !== null) return ` DEFAULT ${formattedDefault}`;
         }
        return "";
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
        const fkConstraint = isOutputTable
            ? `, FOREIGN KEY(id) REFERENCES ${inputTable}(id) ON DELETE CASCADE`
            : "";

        return `CREATE TABLE IF NOT EXISTS ${tableName} (${columns.join(", ")}${fkConstraint})`;
    }

    async function createAutomaticIndexes(dbInstance: Database, tblName: string, schema: z.ZodObject<any>): Promise<void> {
        // Wrap in try-catch as PRAGMA can fail if table doesn't exist yet (e.g., first init)
        try {
            const tableInfo: { name: string }[] = dbInstance.query(`PRAGMA table_info(${tblName})`).all() as { name: string }[];
            for (const column of tableInfo) {
                if (column.name === "id" || !schema.shape[column.name]) continue;

                const zodType = schema.shape[column.name];
                const sqlType = getSqliteType(zodType);
                // Index numeric, integer, enum, and date columns by default
                if (sqlType === "NUMERIC" || sqlType === "INTEGER" || isZodEnum(zodType) || isZodDate(zodType)) {
                    const indexName = `idx_${tblName}_${column.name}`;
                    try {
                        dbInstance.query(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${tblName}(${column.name})`).run();
                    } catch (error) {
                        console.warn(`[Database Indexing ${baseName}] Failed to create index ${indexName}:`, error);
                    }
                }
            }
        } catch (error) {
             if (!error.message.includes("no such table")) {
                 console.error(`[Database Indexing ${baseName}] Error getting table info for ${tblName}:`, error);
             }
             // Ignore "no such table" as CREATE TABLE will handle it
        }
    }

    function processValueForStorage(value: any, zodType: z.ZodTypeAny): any {
        if (value === undefined || value === null) return null;
        const unwrapped = zodType.unwrap ? zodType.unwrap() : zodType;

        if (value instanceof Date) return value.toISOString();
        if (isZodBoolean(unwrapped)) return value ? 1 : 0;
        if (isZodArray(unwrapped) || isZodObject(unwrapped)) return JSON.stringify(value);
        if (isZodNumber(unwrapped) && typeof value === 'number' && !Number.isFinite(value)) {
             console.warn(`[Database Storage ${baseName}] Attempted to store non-finite number: ${value}. Storing as NULL.`);
             return null;
        }
        return value;
    }

     function processObjectForStorage(data: Record<string, any>, schema: z.ZodObject<any>): Record<string, any> {
        const result: Record<string, any> = {};
        for (const [key, value] of Object.entries(data)) {
            if (schema.shape[key]) {
                 result[key] = processValueForStorage(value, schema.shape[key]);
            }
        }
        return result;
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
        } catch (e) {
            console.error(`[Database Deserialization ${baseName}] Error deserializing value "${value}" for type ${unwrapped?._def?.typeName}:`, e);
            return undefined; // Indicate failure
        }
        return value;
    }
    
    // **MODIFIED**: Now returns raw deserialized data, NO Zod parsing here
    function mapRowToRawRecord(row: any): RawDatabaseRecord | null {
        if (!row || typeof row.id !== 'string') return null;

        const rawInputData: Record<string, any> = {};
        const rawOutputData: Record<string, any> = {};
        let hasOutputField = false;

        for (const [key, dbValue] of Object.entries(row)) {
            if (key === 'id') continue;

            if (key in inputSchema.shape) {
                // Basic deserialization based on schema type hint
                rawInputData[key] = deserializeValue(dbValue, inputSchema.shape[key]);
            } else if (key in outputSchema.shape) {
                 // Basic deserialization based on schema type hint
                const deserialized = deserializeValue(dbValue, outputSchema.shape[key]);
                 if (deserialized !== null) { hasOutputField = true; }
                rawOutputData[key] = deserialized;
            }
        }

        return {
            id: row.id,
            input: rawInputData,
            ...(hasOutputField && { output: rawOutputData }),
        };
    }

    function buildWhereClause(
        filter: { input?: Filter<I>, output?: Filter<O> } | undefined,
        params: any[],
        inputAlias: string = 'i',
        outputAlias: string = 'o'
    ): string {
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
        try {
            const tableInfo: { name: string, type: string, notnull: number, dflt_value: any, pk: number }[] = dbInstance.query(`PRAGMA table_info(${tableName})`).all() as any;
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
                        console.log(`[Database Schema ${baseName}] Added missing column ${key} (${columnType}) to table ${tableName}`);
                    } else {
                        console.warn(`[Database Schema ${baseName}] Could not determine SQLite type for column ${key} in ${tableName}`);
                    }
                }
            }
        } catch (error) {
             if (!error.message.includes("no such table")) {
                  console.error(`[Database Schema ${baseName}] Error ensuring columns for table ${tableName}:`, error);
                  throw error;
             }
        }
    }
        const setupTriggers = () => {
        db.transaction(() => {
            // Changes Table
            db.query(`
              CREATE TABLE IF NOT EXISTS ${changesTable} (
                change_id INTEGER PRIMARY KEY AUTOINCREMENT, record_id TEXT NOT NULL,
                change_type TEXT NOT NULL, table_name TEXT NOT NULL,
                changed_at INTEGER NOT NULL DEFAULT (unixepoch()) )
            `).run();
            // Input Insert Trigger
            db.query(`DROP TRIGGER IF EXISTS ${inputTable}_insert_trigger`).run(); // Drop first for idempotency
            db.query(`
              CREATE TRIGGER ${inputTable}_insert_trigger AFTER INSERT ON ${inputTable} BEGIN
                INSERT INTO ${changesTable} (record_id, change_type, table_name) VALUES (NEW.id, 'input_added', 'input');
              END;
            `).run();
            // Input Delete Trigger
            db.query(`DROP TRIGGER IF EXISTS ${inputTable}_delete_trigger`).run();
            db.query(`
              CREATE TRIGGER ${inputTable}_delete_trigger AFTER DELETE ON ${inputTable} BEGIN
                INSERT INTO ${changesTable} (record_id, change_type, table_name) VALUES (OLD.id, 'record_deleted', 'input');
              END;
            `).run();
            // Output Insert Trigger
            db.query(`DROP TRIGGER IF EXISTS ${outputTable}_insert_trigger`).run();
            db.query(`
              CREATE TRIGGER ${outputTable}_insert_trigger AFTER INSERT ON ${outputTable} BEGIN
                INSERT INTO ${changesTable} (record_id, change_type, table_name) VALUES (NEW.id, 'output_added', 'output');
              END;
            `).run();
            // Output Update Trigger
            db.query(`DROP TRIGGER IF EXISTS ${outputTable}_update_trigger`).run();
            db.query(`
              CREATE TRIGGER ${outputTable}_update_trigger AFTER UPDATE ON ${outputTable} BEGIN
                INSERT INTO ${changesTable} (record_id, change_type, table_name) VALUES (NEW.id, 'record_updated', 'output');
              END;
            `).run();
        })();
    };
    // **MODIFIED**: Now fetches raw record and parses *before* emitting
    const getRawRecordByIdInternal = async (id: string): Promise<RawDatabaseRecord | null> => {
         const row = db.query(`
            SELECT i.*, o.* FROM ${inputTable} i LEFT JOIN ${outputTable} o ON i.id = o.id WHERE i.id = ?
        `).get(id);
        // Returns raw record, no Zod parsing here
        return mapRowToRawRecord(row);
    };

    // **MODIFIED**: Fetches raw record, parses, then emits ParsedChangeEvent
    const pollChanges = async () => {
        if (!db || db.closed) {
             if (listenerStopHandle) listenerStopHandle.stop();
             return;
        }
        try {
            const changes: {change_id: number, record_id: string, change_type: string, table_name: string}[] = db.query(`
                SELECT change_id, record_id, change_type, table_name FROM ${changesTable}
                WHERE change_id > ? ORDER BY change_id ASC
            `).all(lastChangeId) as any;

            if (changes.length === 0) return;
            lastChangeId = changes[changes.length - 1].change_id;

            for (const change of changes) {
                let rawRecord: RawDatabaseRecord | null = null;
                let parsedInput: z.infer<I> | undefined = undefined;
                let parsedOutput: z.infer<O> | undefined = undefined;

                 if (change.change_type !== 'record_deleted') {
                     try {
                         rawRecord = await getRawRecordByIdInternal(change.record_id);
                         if (rawRecord) {
                             // Attempt to parse the raw data here before emitting
                             try {
                                 parsedInput = inputSchema.parse(rawRecord.input);
                             } catch (e) {
                                 console.error(`[Listener ${baseName}] Failed to parse input for ID ${change.record_id}:`, e);
                             }
                             if (rawRecord.output) {
                                 try {
                                     parsedOutput = outputSchema.parse(rawRecord.output);
                                 } catch (e) {
                                     console.error(`[Listener ${baseName}] Failed to parse output for ID ${change.record_id}:`, e);
                                 }
                             }
                         }
                     } catch (fetchError) { /* ... error handling ... */ }
                 }

                let eventType: ParsedChangeEvent<any, any>['type'] | null = change.change_type as any;
                if (eventType) {
                     // Emit event with *parsed* data (if parsing succeeded)
                     const eventData: ParsedChangeEvent<z.infer<I>, z.infer<O>> = {
                        type: eventType, id: change.record_id,
                        ...(parsedInput && { input: parsedInput }),
                        ...(parsedOutput && { output: parsedOutput }),
                    };
                    eventEmitter.emit('change', eventData);
                }
            }
        } catch (error) { /* ... error handling ... */ }
    };

    // Function to stringify schema shape for storage/comparison
    // Using simple JSON.stringify on shape keys sorted alphabetically
    const stringifySchemaShape = (schema: z.ZodObject<any>): string => {
        const shape = schema.shape;
        const sortedKeys = Object.keys(shape).sort();
        const shapeRepresentation: Record<string, string> = {};
        for (const key of sortedKeys) {
            // Represent type simply by its name (e.g., "ZodString", "ZodNumber")
            // This is a basic representation, might need refinement for complex types
            shapeRepresentation[key] = shape[key]._def.typeName;
        }
        return JSON.stringify(shapeRepresentation);
    };

    // --- Internal API Implementation ---
    const internalApi: SatiDatabase<I, O> = {
        async init(): Promise<void> {
            if (initialized) return;
            db.transaction(() => {
                // Create core tables
                db.query(createTableSql(inputSchema, inputTable, false)).run();
                db.query(createTableSql(outputSchema, outputTable, true)).run();

                // Create metadata table
                db.query(`
                    CREATE TABLE IF NOT EXISTS ${metadataTable} (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                `).run();

                // Check / Store Schema Shape
                const currentInputShapeStr = stringifySchemaShape(inputSchema);
                const currentOutputShapeStr = stringifySchemaShape(outputSchema);
                const storedInputShape = db.query<{ value: string }>(`SELECT value FROM ${metadataTable} WHERE key = 'input_schema_shape'`).get()?.value;
                const storedOutputShape = db.query<{ value: string }>(`SELECT value FROM ${metadataTable} WHERE key = 'output_schema_shape'`).get()?.value;

                if (storedInputShape === undefined || storedOutputShape === undefined) {
                    // First time init or metadata missing, store current schemas
                    console.log(`[Database ${baseName}] Storing schema shapes for the first time.`);
                    db.query(`INSERT OR REPLACE INTO ${metadataTable} (key, value) VALUES (?, ?)`).run('input_schema_shape', currentInputShapeStr);
                    db.query(`INSERT OR REPLACE INTO ${metadataTable} (key, value) VALUES (?, ?)`).run('output_schema_shape', currentOutputShapeStr);
                } else {
                    // Compare stored vs current
                    if (storedInputShape !== currentInputShapeStr) {
                        throw new Error(`[Database ${baseName}] Schema mismatch: Provided input schema shape differs from the one stored in the database metadata table '${metadataTable}'.`);
                    }
                    if (storedOutputShape !== currentOutputShapeStr) {
                        throw new Error(`[Database ${baseName}] Schema mismatch: Provided output schema shape differs from the one stored in the database metadata table '${metadataTable}'.`);
                    }
                    console.log(`[Database ${baseName}] Schema shapes verified successfully.`);
                }

                // Ensure columns and indexes after schema check
                ensureTableColumns(db, inputTable, inputSchema, false);
                ensureTableColumns(db, outputTable, outputSchema, true);
                createAutomaticIndexes(db, inputTable, inputSchema).catch(e => console.error(`Index creation error: ${e}`));
                setupTriggers();
            })();
            initialized = true;
            console.log(`[Database ${baseName}] Initialized (Tables: ${inputTable}, ${outputTable}).`);
        },

        // **MODIFIED**: Uses INSERT OR IGNORE for output, returns indicator
        async insert(input: z.infer<I>, output?: z.infer<O>): Promise<{ id: string, output_ignored?: boolean }> {
            if (!initialized) await internalApi.init();
            const validatedInput = inputSchema.parse(input);
            const id = generateRecordId(validatedInput, inputSchema);
            let validatedOutput: z.infer<O> | undefined = undefined;
            if (output) validatedOutput = outputSchema.parse(output);
            let output_ignored = false;

            db.transaction(() => {
                const processedInput = processObjectForStorage(validatedInput, inputSchema);
                const inputColumns = Object.keys(processedInput);
                const inputPlaceholders = inputColumns.map(() => "?").join(", ");
                db.query(`INSERT INTO ${inputTable} (id, ${inputColumns.join(", ")}) VALUES (?, ${inputPlaceholders}) ON CONFLICT(id) DO NOTHING`)
                  .run(id, ...Object.values(processedInput));

                if (validatedOutput) {
                    const processedOutput = processObjectForStorage(validatedOutput, outputSchema);
                    const outputColumns = Object.keys(processedOutput);
                    if (outputColumns.length > 0) {
                        const outputPlaceholders = outputColumns.map(() => "?").join(", ");
                        // Use INSERT OR IGNORE for output - prevents overwriting
                        const result = db.query(`
                            INSERT OR IGNORE INTO ${outputTable} (id, ${outputColumns.join(", ")})
                            VALUES (?, ${outputPlaceholders})
                        `).run(id, ...Object.values(processedOutput));
                        // If changes = 0, it means the row already existed and was ignored
                        if (result.changes === 0) {
                            output_ignored = true;
                        }
                    } else {
                         // If output object is empty, ensure no output record exists (or ignore if one does?)
                         // Let's keep delete here for consistency if output becomes empty.
                         db.query(`DELETE FROM ${outputTable} WHERE id = ?`).run(id);
                    }
                }
            })();
            if (output_ignored) {
                 console.log(`[Database ${baseName}] Output for ID ${id} already existed and was ignored.`);
            }
            return { id, output_ignored };
        },

        // **MODIFIED**: Returns raw records
        async findRaw(filter): Promise<Array<RawDatabaseRecord>> {
             if (!initialized) await internalApi.init();
             const params: any[] = [];
             let sql = `SELECT i.*, o.* FROM ${inputTable} i LEFT JOIN ${outputTable} o ON i.id = o.id`;
             const whereClause = buildWhereClause(filter, params, 'i', 'o');
             sql += ` ${whereClause}`;
             if (filter?.output && Object.keys(filter.output).length > 0) {
                 sql += (whereClause ? " AND" : " WHERE") + " o.id IS NOT NULL";
             }
             const results = db.query(sql).all(...params);
             // Map to raw records, no Zod parsing
             return results.map(row => mapRowToRawRecord(row)).filter(record => record !== null) as Array<RawDatabaseRecord>;
        },

        // **MODIFIED**: Returns raw record
        async findByIdRaw(id: string): Promise<RawDatabaseRecord | null> {
            if (!initialized) await internalApi.init();
            return getRawRecordByIdInternal(id); // Uses internal raw fetch
        },

        // **MODIFIED**: Throws error if output already exists
        async update(input: z.infer<I>, output: Partial<z.infer<O>>): Promise<{ id: string }> {
             if (!initialized) await internalApi.init();
             const validatedInput = inputSchema.parse(input);
             const id = generateRecordId(validatedInput, inputSchema);
             const validatedOutputUpdate = outputSchema.partial().parse(output);
             if (Object.keys(validatedOutputUpdate).length === 0) return { id };

             const inputExists = db.query(`SELECT 1 FROM ${inputTable} WHERE id = ?`).get(id);
             if (!inputExists) throw new Error(`Update failed: Input ID ${id} not found.`);

             // Check if output *already* exists before attempting update
             const existingOutputRow = db.query(`SELECT 1 FROM ${outputTable} WHERE id = ?`).get(id);
             if (existingOutputRow) {
                 throw new Error(`Update failed: Output already exists for ID ${id}. Use insert (which ignores existing) or delete first.`);
             }

             // If output doesn't exist, proceed as an insert (effectively)
             const mergedOutput = { ...validatedOutputUpdate }; // No existing data to merge
             const validatedMergedOutput = outputSchema.parse(mergedOutput);
             const processedOutput = processObjectForStorage(validatedMergedOutput, outputSchema);
             const columns = Object.keys(processedOutput);

             if (columns.length > 0) {
                 const placeholders = columns.map(() => "?").join(", ");
                 // Use INSERT OR IGNORE here too, although check above should prevent conflict
                 db.query(`INSERT OR IGNORE INTO ${outputTable} (id, ${columns.join(", ")}) VALUES (?, ${placeholders})`)
                   .run(id, ...Object.values(processedOutput));
             }
             // No delete case needed as we checked for non-existence

             return { id };
        },


        async delete(input: z.infer<I>): Promise<{ id: string }> {
            if (!initialized) await internalApi.init();
            const validatedInput = inputSchema.parse(input);
            const id = generateRecordId(validatedInput, inputSchema);
            const result = db.query(`DELETE FROM ${inputTable} WHERE id = ?`).run(id); // Cascade handles output
            if (result.changes === 0) throw new Error(`Delete failed: Record ID ${id} not found.`);
            return { id };
        },

        async query<T = any>(sql: string, ...params: any[]): Promise<T[]> {
             if (!initialized) await internalApi.init();
            // Prepare statement for safety against SQL injection in params
            let stmt: Statement;
            try {
                 stmt = db.prepare(sql);
                 return stmt.all(...params) as T[];
            } catch (error) {
                 console.error(`[Database Query ${baseName}] Error executing query "${sql}" with params ${JSON.stringify(params)}:`, error);
                 throw error; // Re-throw error after logging
            }
        },

        listen(callback: (event: ChangeEvent<z.infer<I>, z.infer<O>>) => void): { stop: () => void } {
            if (!initialized) {
                this.init().catch(error => console.error(`[DB Listener ${baseName}] Init error:`, error));
            }
            eventEmitter.on('change', callback);
            activeListenersCount++;
            if (!listenerStopHandle && activeListenersCount > 0) {
                const lastChangeRecord = db.query(`SELECT MAX(change_id) as id FROM ${changesTable}`).get();
                lastChangeId = lastChangeRecord?.id || 0;
                const intervalId = setInterval(pollChanges, 500); // Poll interval
                console.log(`[DB Listener ${baseName}] Polling started.`);
                listenerStopHandle = {
                     stop: () => {
                         clearInterval(intervalId);
                         listenerStopHandle = null;
                         console.log(`[DB Listener ${baseName}] Polling stopped.`);
                     }
                };
            }
            return {
                stop: () => {
                    eventEmitter.off('change', callback);
                    activeListenersCount--;
                    console.log(`[DB Listener ${baseName}] Callback removed. Active listeners: ${activeListenersCount}`);
                    if (activeListenersCount === 0 && listenerStopHandle) {
                        listenerStopHandle.stop();
                    }
                }
            };
        },
                getInputSchema: () => inputSchema,
        getOutputSchema: () => outputSchema,
        getName: () => baseName,
        _generateId: (input: z.infer<I>) => generateRecordId(input, inputSchema),
        _getDbInstance: () => db,
        close(): void {
            if (listenerStopHandle) {
                try { listenerStopHandle.stop(); } catch (e) { /* ignore */ }
                listenerStopHandle = null;
            }
            if (db && !db.closed) { // Check if DB is already closed
                 try { db.close(); } catch (e) { console.error(`[Database ${baseName}] Error closing DB:`, e); }
            }
            eventEmitter.removeAllListeners('change');
            activeListenersCount = 0;
            initialized = false; // Reset initialized state
            console.log(`[Database ${baseName}] Closed.`);
       }    };
    // Ensure full implementation of simplified methods above is present
    return internalApi;
}


// --- New Interface Function: Requester ---
export interface SatiInsertProcessor<I extends z.ZodObject<any>, O extends z.ZodObject<any>> {
    process: (input: z.infer<I>) => Promise<z.infer<O>>;
    stop: () => void;
}
interface PendingPromise<O> {
    resolve: (output: O) => void;
    reject: (reason?: any) => void;
    timer: NodeJS.Timeout;
}
export function useDatabaseAsInsertFunction<I extends z.ZodObject<any>, O extends z.ZodObject<any>>(
    dbPath: string,
    inputSchema: I,
    outputSchema: O,
    options?: { timeoutMs?: number }
): SatiInsertProcessor<I, O> {
    const db = useDatabaseInternal(dbPath, inputSchema, outputSchema);
    const pendingPromises = new Map<string, PendingPromise<z.infer<O>>>();
    const defaultTimeoutMs = options?.timeoutMs ?? 30000;
    let listenerHandle: { stop: () => void } | null = null;
    let isStopping = false;

    const startListener = () => {
        if (listenerHandle || isStopping) return;
        // Listener now receives ParsedChangeEvent
        listenerHandle = db.listen((event) => {
            if ((event.type === 'output_added' || event.type === 'record_updated') && event.id) {
                const pending = pendingPromises.get(event.id);
                // Event already contains parsed output (if parsing succeeded in pollChanges)
                if (pending && event.output) {
                    clearTimeout(pending.timer);
                    // No need to parse again here
                    pending.resolve(event.output);
                    pendingPromises.delete(event.id);
                } else if (pending && !event.output) {
                     // This case might happen if output was added but failed parsing in pollChanges
                     console.warn(`[InsertFunction ${db.getName()}] Received event for ID ${event.id} but output parsing failed internally.`);
                     // Potentially reject or keep waiting? Rejecting is safer.
                     clearTimeout(pending.timer);
                     pending.reject(new Error(`Internal parsing failed for output of ID ${event.id}. Check listener logs.`));
                     pendingPromises.delete(event.id);
                }
            } else if (event.type === 'record_deleted' && event.id) {
                const pending = pendingPromises.get(event.id);
                if (pending) {
                    clearTimeout(pending.timer);
                    pending.reject(new Error(`Record ${event.id} deleted while awaiting output.`));
                    pendingPromises.delete(event.id);
                }
            }
        });
        console.log(`[InsertFunction ${db.getName()}] Listener started.`);
    };

    async function process(input: z.infer<I>): Promise<z.infer<O>> {
        if (isStopping) throw new Error(`[InsertFunction ${db.getName()}] Processor is stopping.`);
        await db.init(); // Ensures schema check runs
        startListener();

        const validatedInput = inputSchema.parse(input);
        const id = db._generateId(validatedInput);

        // Check pending promises first
        if (pendingPromises.has(id)) {
             console.warn(`[InsertFunction ${db.getName()}] Input ID ${id} already pending. Re-entering wait.`);
        }

        // **MODIFIED**: Use findByIdRaw and parse here
        const rawRecord = await db.findByIdRaw(id);
        if (rawRecord?.output) {
            try {
                // Parse the raw output here
                const validatedOutput = outputSchema.parse(rawRecord.output);
                console.log(`[InsertFunction ${db.getName()}] Found existing valid output for ID: ${id}`);
                return validatedOutput;
            } catch (e) {
                console.error(`[InsertFunction ${db.getName()}] Found existing output for ID ${id}, but failed validation:`, e);
                // Decide if we should still wait or throw. Let's wait for potentially valid update.
            }
        }

        console.log(`[InsertFunction ${db.getName()}] Waiting for output for ID: ${id}`);
        await db.insert(validatedInput); // Ensure input exists

        return new Promise<z.infer<O>>((resolve, reject) => {
            const existingPending = pendingPromises.get(id);
            if (existingPending) clearTimeout(existingPending.timer);

            const timer = setTimeout(() => {
                pendingPromises.delete(id);
                reject(new Error(`Timeout waiting for output for ID ${id} after ${defaultTimeoutMs}ms`));
            }, defaultTimeoutMs);
            pendingPromises.set(id, { resolve, reject, timer });
        });
    }

    function stop() {
        if (isStopping) return;
        isStopping = true;
        console.log(`[InsertFunction ${db.getName()}] Stopping...`);
        if (listenerHandle) {
            try { listenerHandle.stop(); } catch(e) {/* ignore */}
            listenerHandle = null;
        }
        pendingPromises.forEach((pending, id) => {
            clearTimeout(pending.timer);
            pending.reject(new Error(`Processor stopped while waiting for output for ID ${id}.`));
        });
        pendingPromises.clear();
        // Close the underlying DB connection only if this processor owns it
        // Requires connection management logic (TODO)
        try { db.close(); } catch(e) {/* ignore */}
        console.log(`[InsertFunction ${db.getName()}] Stopped.`);
    }

    return { process, stop };
}


// --- New Interface Function: Worker/Listener ---
export type SatiListenerHandler<I, O> = (
    input: I, // Handler receives parsed input
    setOutput: (output: O) => Promise<{ id: string, output_ignored?: boolean }>, // setOutput returns insert result
    event: ParsedChangeEvent<I, O> // Event contains parsed data
) => Promise<void> | void;

export interface SatiListenerControl {
    stop: () => void;
}

export function useDatabaseAsListener<I extends z.ZodObject<any>, O extends z.ZodObject<any>>(
    dbPath: string,
    inputSchema: I,
    outputSchema: O,
    handlerCallback: SatiListenerHandler<z.infer<I>, z.infer<O>>,
    options?: { maxConcurrency?: number }
): SatiListenerControl {
    const db = useDatabaseInternal(dbPath, inputSchema, outputSchema);
    let listenerHandle: { stop: () => void } | null = null;
    let activeHandlers = 0;
    const maxConcurrency = options?.maxConcurrency ?? 1;
    const workQueue: ParsedChangeEvent<z.infer<I>, z.infer<O>>[] = []; // Queue stores parsed events
    let isProcessingQueue = false;
    let isStopping = false;

    // **MODIFIED**: Receives ParsedChangeEvent
    const processEvent = async (event: ParsedChangeEvent<z.infer<I>, z.infer<O>>) => {
         if (isStopping) return;
         // Only process input_added events where input parsing succeeded
         if (event.type === 'input_added' && event.id && event.input) {
             workQueue.push(event);
             triggerQueueProcessing();
         }
    };

    const triggerQueueProcessing = async () => {
        if (isProcessingQueue || isStopping || workQueue.length === 0) return;
        isProcessingQueue = true;
        while (workQueue.length > 0 && activeHandlers < maxConcurrency && !isStopping) {
             const event = workQueue.shift();
             if (!event) continue; // Should not happen

             const inputId = event.id;
             const parsedInput = event.input!; // Already parsed by pollChanges

             // **MODIFIED**: Use findByIdRaw
             try {
                 const rawExisting = await db.findByIdRaw(inputId);
                 // Check if raw output exists (don't parse here unnecessarily)
                 if (rawExisting?.output) {
                     console.log(`[Listener ${db.getName()}] Output already exists raw for queued [${inputId}]. Skipping.`);
                     continue;
                 }
                } catch (e) {
                    console.error(`[Listener ${db.getName()}] Error checking existing output for queued ${inputId}:`, e);
                    continue; // Skip on error
                }
   
             activeHandlers++;
             console.log(`[Listener ${db.getName()}] Starting handler for ID: ${inputId}. Active: ${activeHandlers}/${maxConcurrency}`);

             // **MODIFIED**: setOutput parses output and uses INSERT OR IGNORE internally
             const setOutput = async (outputData: z.infer<O>): Promise<{ id: string, output_ignored?: boolean }> => {
                 const validatedOutput = outputSchema.parse(outputData); // Parse before insert
                 // db.insert handles INSERT OR IGNORE for output
                 return db.insert(parsedInput, validatedOutput);
             };

             // Execute handler (receives parsed input)
             handlerCallback(parsedInput, setOutput, event)
                 .then(() => {
                     console.log(`[Listener ${db.getName()}] Handler finished successfully for ID: ${inputId}.`);
                 })
                 .catch(error => {
                     console.error(`[Listener ${db.getName()}] Error executing handler for input ID ${inputId}:`, error);
                 })
                 .finally(() => {
                     activeHandlers--;
                     console.log(`[Listener ${db.getName()}] Handler ended for ID: ${inputId}. Active: ${activeHandlers}/${maxConcurrency}`);
                     // Trigger processing again in case queue has items and slots are free
                     if (!isStopping) triggerQueueProcessing();
                 });
        }
        isProcessingQueue = false;
        if (!isStopping && workQueue.length > 0 && activeHandlers < maxConcurrency) {
             triggerQueueProcessing();
        }
    };

    // Initialize and start listening
    db.init()
        .then(() => {
            if (isStopping) return;
            listenerHandle = db.listen(processEvent); // processEvent handles parsed events
            console.log(`[Listener ${db.getName()}] Started listening for inputs.`);
        })
        .catch(error => {
            console.error(`[Listener ${db.getName()}] Failed to initialize database:`, error);
        });

        return {
            stop: () => {
                if (isStopping) return;
                isStopping = true;
                console.log(`[Listener ${db.getName()}] Stopping...`);
                if (listenerHandle) {
                    try { listenerHandle.stop(); } catch(e) {/* ignore */}
                    listenerHandle = null;
                }
                workQueue.length = 0; // Clear pending queue
                // Note: Active handlers will complete, but no new ones will start.
                // Consider adding logic to wait for active handlers if needed.
                try { db.close(); } catch(e) {/* ignore */} // Close DB if owned (requires management)
                console.log(`[Listener ${db.getName()}] Stopped.`);
            }
        };
    }

// NOTE: Need to copy the full implementation of utility functions into useDatabaseInternal
// from the `sati_db_async_processor_v1` artifact for this code to be fully functional.
// They were simplified above for brevity in the diff.
