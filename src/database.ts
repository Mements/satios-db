import { Database, Statement } from "bun:sqlite"; // Import Statement type if needed
import { z } from "zod";
import path from "path";
import fs from "node:fs";
import { createHash } from "node:crypto";
import { EventEmitter } from "events";

// Define or import RESERVED_SQLITE_WORDS robustly
const RESERVED_SQLITE_WORDS = new Set(["ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE", "AND", "AS", "ASC", "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT", "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DO", "DROP", "EACH", "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUDE", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FILTER", "FIRST", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL", "GENERATED", "GLOB", "GROUP", "GROUPS", "HAVING", "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LAST", "LEFT", "LIKE", "LIMIT", "MATCH", "MATERIALIZED", "NATURAL", "NO", "NOT", "NOTHING", "NOTNULL", "NULL", "NULLS", "OF", "OFFSET", "ON", "OR", "ORDER", "OTHERS", "OUTER", "OVER", "PARTITION", "PLAN", "PRAGMA", "PRECEDING", "PRIMARY", "QUERY", "RAISE", "RANGE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RIGHT", "ROLLBACK", "ROW", "ROWS", "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION", "TRIGGER", "UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE", "WINDOW", "WITH", "WITHOUT"]);

// --- Base Database Interfaces ---
export interface DatabaseRecord<I, O> {
    id: string; // Hash of the input object
    input: I;
    output?: O; // Output is optional
}

export interface ChangeEvent<I, O> {
    type: 'input_added' | 'output_added' | 'record_updated' | 'record_deleted';
    id: string;
    input?: I;
    output?: O;
}

export type RangeFilter<T> = [T, T];
export type Filter<T extends z.ZodObject<any>> = Partial<z.infer<T> & {
    [K in keyof z.infer<T>]?: z.infer<T>[K] | RangeFilter<z.infer<T>[K]>
}>;

// Interface for the core database operations (internal)
interface SatiDatabase<I extends z.ZodObject<any>, O extends z.ZodObject<any>> {
    init: () => Promise<void>;
    insert: (input: z.infer<I>, output?: z.infer<O>) => Promise<{ id: string }>;
    find: (filter: { input?: Filter<I>, output?: Filter<O> }) => Promise<Array<DatabaseRecord<z.infer<I>, z.infer<O>>>>;
    findById: (id: string) => Promise<DatabaseRecord<z.infer<I>, z.infer<O>> | null>;
    update: (input: z.infer<I>, output: Partial<z.infer<O>>) => Promise<{ id: string }>;
    delete: (input: z.infer<I>) => Promise<{ id: string }>;
    query: <T = any>(sql: string, ...params: any[]) => Promise<T[]>;
    listen: (callback: (event: ChangeEvent<z.infer<I>, z.infer<O>>) => void) => { stop: () => void };
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
     const orderedInput = Object.keys(input)
         .sort()
         .reduce((obj, key) => {
             let value = input[key];
             if (value !== null && typeof value === 'object') {
                  try {
                      value = JSON.stringify(value, (k, v) => {
                          if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
                              return Object.keys(v).sort().reduce((sortedObj, innerKey) => {
                                  sortedObj[innerKey] = v[innerKey];
                                  return sortedObj;
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

    if ('id' in inputSchema.shape || 'id' in outputSchema.shape) {
        throw new Error(`[Database Setup ${baseName}] Schemas cannot contain 'id'.`);
    }

    const dir = path.dirname(dbPath);
    try { fs.mkdirSync(dir, { recursive: true }); } catch (error: any) { if (error.code !== 'EEXIST') throw error; }

    const db = new Database(dbPath);
    // Enable WAL mode for better concurrency
    try {
        db.exec("PRAGMA journal_mode = WAL;");
    } catch (e) {
        console.warn(`[Database ${baseName}] Could not enable WAL mode:`, e.message);
    }
    let initialized = false;
    const eventEmitter = new EventEmitter();
    let listenerStopHandle: { stop: () => void } | null = null;
    let activeListenersCount = 0;
    let lastChangeId = 0; // Used for polling


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

     function mapRowToRecord(row: any): DatabaseRecord<z.infer<I>, z.infer<O>> | null {
        if (!row || typeof row.id !== 'string') return null;

        const inputData: Record<string, any> = {};
        const outputData: Record<string, any> = {};
        let hasOutputField = false;

        for (const [key, dbValue] of Object.entries(row)) {
            if (key === 'id') continue;

            if (key in inputSchema.shape) {
                inputData[key] = deserializeValue(dbValue, inputSchema.shape[key]);
            } else if (key in outputSchema.shape) {
                const deserialized = deserializeValue(dbValue, outputSchema.shape[key]);
                 if (deserialized !== null) { hasOutputField = true; }
                outputData[key] = deserialized;
            }
        }

        try {
            const validatedInput = inputSchema.parse(inputData);
            const validatedOutput = hasOutputField ? outputSchema.parse(outputData) : undefined;

            return {
                id: row.id,
                input: validatedInput,
                ...(validatedOutput !== undefined && { output: validatedOutput }),
            };
        } catch (parseError) {
            console.error(`[Database ${baseName}] Failed to parse row with id ${row.id}:`, parseError, { inputData, outputData });
            return null;
        }
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

    const getRecordByIdInternal = async (id: string): Promise<DatabaseRecord<z.infer<I>, z.infer<O>> | null> => {
         const row = db.query(`
            SELECT i.*, o.* FROM ${inputTable} i LEFT JOIN ${outputTable} o ON i.id = o.id WHERE i.id = ?
        `).get(id);
        return mapRowToRecord(row);
    };

    const pollChanges = async () => {
        if (!db || db.closed) { // Check if DB is closed
             console.warn(`[Database Listener ${baseName}] DB closed, stopping polling.`);
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
                let record: DatabaseRecord<any, any> | null = null;
                 if (change.change_type !== 'record_deleted') {
                     // Use a separate try-catch here in case the DB is closed mid-poll
                     try {
                         record = await getRecordByIdInternal(change.record_id);
                     } catch (fetchError) {
                         if (fetchError.message.includes("database is closed")) {
                              console.warn(`[Database Listener ${baseName}] DB closed during record fetch for ${change.record_id}.`);
                              continue; // Skip this change if DB closed
                         } else {
                              console.error(`[Database Listener ${baseName}] Error fetching record ${change.record_id}:`, fetchError);
                         }
                     }
                 }

                let eventType: ChangeEvent<any, any>['type'] | null = change.change_type as any;
                if (eventType) {
                     const eventData: ChangeEvent<z.infer<I>, z.infer<O>> = {
                        type: eventType, id: change.record_id,
                        ...(record && { input: record.input }),
                        ...(record && record.output && { output: record.output }),
                    };
                    eventEmitter.emit('change', eventData);
                }
            }
        } catch (error) {
             if (error.message.includes("database is closed")) {
                 console.warn(`[Database Listener ${baseName}] DB closed during polling.`);
                 if (listenerStopHandle) listenerStopHandle.stop();
             } else {
                 console.error(`[Database Listener ${baseName}] Error polling changes:`, error);
             }
        }
    };


    // --- Internal API Implementation ---
    const internalApi: SatiDatabase<I, O> = {
        async init(): Promise<void> {
            if (initialized) return;
            db.transaction(() => {
                db.query(createTableSql(inputSchema, inputTable, false)).run();
                db.query(createTableSql(outputSchema, outputTable, true)).run();
                ensureTableColumns(db, inputTable, inputSchema, false);
                ensureTableColumns(db, outputTable, outputSchema, true);
                 // Run index creation async but don't await here
                 createAutomaticIndexes(db, inputTable, inputSchema).catch(e => console.error(`Index creation error: ${e}`));
                setupTriggers();
            })();
            initialized = true;
            console.log(`[Database ${baseName}] Initialized (Tables: ${inputTable}, ${outputTable}).`);
        },

        async insert(input: z.infer<I>, output?: z.infer<O>): Promise<{ id: string }> {
            if (!initialized) await internalApi.init();
            const validatedInput = inputSchema.parse(input);
            const id = generateRecordId(validatedInput, inputSchema);
            let validatedOutput: z.infer<O> | undefined = undefined;
            if (output) validatedOutput = outputSchema.parse(output);

            db.transaction(() => {
                const processedInput = processObjectForStorage(validatedInput, inputSchema);
                const inputColumns = Object.keys(processedInput);
                const inputPlaceholders = inputColumns.map(() => "?").join(", ");
                db.query(`
                    INSERT INTO ${inputTable} (id, ${inputColumns.join(", ")}) VALUES (?, ${inputPlaceholders})
                    ON CONFLICT(id) DO NOTHING
                `).run(id, ...Object.values(processedInput));

                if (validatedOutput) {
                    const processedOutput = processObjectForStorage(validatedOutput, outputSchema);
                    const outputColumns = Object.keys(processedOutput);
                    if (outputColumns.length > 0) {
                        const outputPlaceholders = outputColumns.map(() => "?").join(", ");
                        db.query(`
                            INSERT OR REPLACE INTO ${outputTable} (id, ${outputColumns.join(", ")})
                            VALUES (?, ${outputPlaceholders})
                        `).run(id, ...Object.values(processedOutput));
                    } else {
                         db.query(`DELETE FROM ${outputTable} WHERE id = ?`).run(id);
                    }
                }
            })();
            return { id };
        },

        async find(filter): Promise<Array<DatabaseRecord<z.infer<I>, z.infer<O>>>> {
             if (!initialized) await internalApi.init();
             const params: any[] = [];
             let sql = `SELECT i.*, o.* FROM ${inputTable} i LEFT JOIN ${outputTable} o ON i.id = o.id`;
             const whereClause = buildWhereClause(filter, params, 'i', 'o');
             sql += ` ${whereClause}`;
             if (filter?.output && Object.keys(filter.output).length > 0) {
                 sql += (whereClause ? " AND" : " WHERE") + " o.id IS NOT NULL";
             }
             const results = db.query(sql).all(...params);
             return results.map(row => mapRowToRecord(row)).filter(record => record !== null) as Array<DatabaseRecord<z.infer<I>, z.infer<O>>>;
        },

        async findById(id: string): Promise<DatabaseRecord<z.infer<I>, z.infer<O>> | null> {
            if (!initialized) await internalApi.init();
            return getRecordByIdInternal(id);
        },

        async update(input: z.infer<I>, output: Partial<z.infer<O>>): Promise<{ id: string }> {
             if (!initialized) await internalApi.init();
             const validatedInput = inputSchema.parse(input);
             const id = generateRecordId(validatedInput, inputSchema);
             const validatedOutputUpdate = outputSchema.partial().parse(output);
             if (Object.keys(validatedOutputUpdate).length === 0) return { id };

             const inputExists = db.query(`SELECT 1 FROM ${inputTable} WHERE id = ?`).get(id);
             if (!inputExists) throw new Error(`Update failed: Input ID ${id} not found.`);

             const existingOutputRow = db.query(`SELECT * FROM ${outputTable} WHERE id = ?`).get(id);
             const existingOutputData = existingOutputRow ? mapRowToRecord({ ...existingOutputRow, id })?.output ?? {} : {};
             const mergedOutput = { ...existingOutputData, ...validatedOutputUpdate };
             const validatedMergedOutput = outputSchema.parse(mergedOutput); // Validate full merged object
             const processedOutput = processObjectForStorage(validatedMergedOutput, outputSchema);
             const columns = Object.keys(processedOutput);

             if (columns.length > 0) {
                 const placeholders = columns.map(() => "?").join(", ");
                 db.query(`INSERT OR REPLACE INTO ${outputTable} (id, ${columns.join(", ")}) VALUES (?, ${placeholders})`)
                   .run(id, ...Object.values(processedOutput));
             } else {
                 db.query(`DELETE FROM ${outputTable} WHERE id = ?`).run(id);
             }
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
        }
    };
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
    // TODO: Implement connection sharing based on dbPath for same process usage
    const db = useDatabaseInternal(dbPath, inputSchema, outputSchema);
    const pendingPromises = new Map<string, PendingPromise<z.infer<O>>>();
    const defaultTimeoutMs = options?.timeoutMs ?? 30000;
    let listenerHandle: { stop: () => void } | null = null;
    let isStopping = false; // Flag to prevent operations during shutdown

    const startListener = () => {
        if (listenerHandle || isStopping) return;
        listenerHandle = db.listen((event) => {
            if ((event.type === 'output_added' || event.type === 'record_updated') && event.id) {
                const pending = pendingPromises.get(event.id);
                if (pending && event.output) {
                    try {
                        const validatedOutput = outputSchema.parse(event.output);
                        clearTimeout(pending.timer);
                        pending.resolve(validatedOutput);
                        pendingPromises.delete(event.id);
                    } catch (validationError) {
                        console.error(`[InsertFunction ${db.getName()}] Invalid output for ID ${event.id}:`, validationError);
                        clearTimeout(pending.timer);
                        pending.reject(new Error(`Invalid output format: ${validationError.message}`));
                        pendingPromises.delete(event.id);
                    }
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
        await db.init();
        startListener();

        const validatedInput = inputSchema.parse(input);
        const id = db._generateId(validatedInput);

        if (pendingPromises.has(id)) {
             console.warn(`[InsertFunction ${db.getName()}] Input ID ${id} already pending. Returning existing wait promise.`);
             // Return a new promise that resolves/rejects when the original does
             // This requires careful handling or might simply await the existing logic again
             // For simplicity, we let it proceed; the listener resolves all waiters.
        }

        const existingRecord = await db.findById(id);
        if (existingRecord?.output) {
            try { return outputSchema.parse(existingRecord.output); }
            catch (e) { console.error(`[InsertFunction ${db.getName()}] Invalid existing output for ID ${id}:`, e); }
        }

        console.log(`[InsertFunction ${db.getName()}] Waiting for output for ID: ${id}`);
        await db.insert(validatedInput); // Ensure input exists

        return new Promise<z.infer<O>>((resolve, reject) => {
            const existingPending = pendingPromises.get(id);
            if (existingPending) clearTimeout(existingPending.timer); // Clear old timer if any

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
    input: I,
    setOutput: (output: O) => Promise<{ id: string }>,
    event: ChangeEvent<I, O>
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
    // TODO: Implement connection sharing based on dbPath
    const db = useDatabaseInternal(dbPath, inputSchema, outputSchema);
    let listenerHandle: { stop: () => void } | null = null;
    let activeHandlers = 0;
    const maxConcurrency = options?.maxConcurrency ?? 1;
    const workQueue: ChangeEvent<z.infer<I>, z.infer<O>>[] = []; // Queue for concurrency control
    let isProcessingQueue = false;
    let isStopping = false; // Flag to prevent new work on stop

    const processEvent = async (event: ChangeEvent<z.infer<I>, z.infer<O>>) => {
         if (isStopping) return; // Don't process if stopping

         if (event.type === 'input_added' && event.id && event.input) {
             workQueue.push(event); // Add to queue
             triggerQueueProcessing(); // Attempt to process queue
         }
    };

    const triggerQueueProcessing = async () => {
        if (isProcessingQueue || isStopping || workQueue.length === 0) return;

        isProcessingQueue = true;
        while (workQueue.length > 0 && activeHandlers < maxConcurrency && !isStopping) {
             const event = workQueue.shift(); // Get next event from queue
             if (!event) continue;

             const inputId = event.id;
             const validatedInput = event.input!; // Already checked in processEvent

             // Double check output doesn't exist *before* starting handler
             try {
                 const existing = await db.findById(inputId);
                 if (existing?.output) {
                     console.log(`[Listener ${db.getName()}] Output exists for queued [${inputId}]. Skipping.`);
                     continue; // Skip this item
                 }
             } catch (e) {
                 console.error(`[Listener ${db.getName()}] Error checking existing output for queued ${inputId}:`, e);
                 continue; // Skip on error
             }

             activeHandlers++;
             console.log(`[Listener ${db.getName()}] Starting handler for ID: ${inputId}. Active: ${activeHandlers}/${maxConcurrency}`);

             // Define setOutput for this specific input
             const setOutput = async (outputData: z.infer<O>): Promise<{ id: string }> => {
                 const validatedOutput = outputSchema.parse(outputData);
                 return db.insert(validatedInput, validatedOutput);
             };

             // Execute handler without awaiting completion here, to allow concurrency
             handlerCallback(validatedInput, setOutput, event)
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
         // Final check in case items were added while processing
         if (!isStopping && workQueue.length > 0 && activeHandlers < maxConcurrency) {
             triggerQueueProcessing();
         }
    };

    // Initialize and start listening
    db.init()
        .then(() => {
            if (isStopping) return; // Don't start if stop was called before init finished
            listenerHandle = db.listen(processEvent);
            console.log(`[Listener ${db.getName()}] Started listening for inputs.`);
        })
        .catch(error => {
            console.error(`[Listener ${db.getName()}] Failed to initialize database:`, error);
        });

    // Return control object
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
