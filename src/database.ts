import { Database } from "bun:sqlite";
import { z } from "zod";
import path from "path";
import fs from "node:fs";
import { createHash } from "node:crypto";

// Type for measure function (same as in tryHelius.ts)
type MeasureFunction = <T>(fn: () => Promise<T>, label: string) => Promise<T>;

// Default no-op measure function
const defaultMeasure: MeasureFunction = async <T>(fn: () => Promise<T>) => await fn();

// Reserved SQLite words
const RESERVED_SQLITE_WORDS = new Set([
  "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE", "AND", "AS", "ASC", "ATTACH", "AUTOINCREMENT",
  "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT", "CONFLICT",
  "CONSTRAINT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE", "DEFAULT",
  "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DO", "DROP", "EACH", "ELSE", "END", "ESCAPE",
  "EXCEPT", "EXCLUDE", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FILTER", "FIRST", "FOLLOWING", "FOR", "FOREIGN", "FROM",
  "FULL", "GENERATED", "GLOB", "GROUP", "GROUPS", "HAVING", "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDEXED",
  "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LAST", "LEFT", "LIKE",
  "LIMIT", "MATCH", "MATERIALIZED", "NATURAL", "NO", "NOT", "NOTHING", "NOTNULL", "NULL", "NULLS", "OF", "OFFSET", "ON",
  "OR", "ORDER", "OTHERS", "OUTER", "OVER", "PARTITION", "PLAN", "PRAGMA", "PRECEDING", "PRIMARY", "QUERY", "RAISE",
  "RANGE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RIGHT", "ROLLBACK",
  "ROW", "ROWS", "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION",
  "TRIGGER", "UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE",
  "WINDOW", "WITH", "WITHOUT"
]);

// Base Types
export type RawDatabaseRecord = {
  id: string;
  input: Record<string, any>;
  output?: Record<string, any>;
};
export type RangeFilter<T> = [T, T];
export type Filter<T extends z.ZodObject<any>> = Partial<z.infer<T> & {
  [K in keyof z.infer<T>]?: z.infer<T>[K] | RangeFilter<z.infer<T>[K]>;
}>;
export interface TableNames {
  inputTable: string;
  outputTable: string;
  changesTable: string;
  metadataTable: string;
  schemaTable: string;
}

// Zod Type Guards
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

// Utility Functions
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
              }, {} as Record<string, any>);
            }
            return v;
          });
        } catch {
          value = JSON.stringify(value);
        }
      } else {
        value = JSON.stringify(value);
      }
      obj[key] = value;
      return obj;
    }, {} as Record<string, any>);
  return createHash('sha256').update(JSON.stringify(orderedInput)).digest('hex').substring(0, 16);
}

function sanitizeName(name: string): string {
  let sanitized = name.replace(/[^a-zA-Z0-9_]/g, "_");
  if (/^[0-9]/.test(sanitized)) sanitized = "t_" + sanitized;
  if (RESERVED_SQLITE_WORDS.has(sanitized.toUpperCase())) sanitized = "t_" + sanitized;
  return sanitized;
}

// SQLite Utility Functions
function getSqliteType(zodType: z.ZodType): string | null {
  const unwrapped = zodType.unwrap ? zodType.unwrap() : zodType;
  if (isZodString(unwrapped) || isZodEnum(unwrapped) || isZodArray(unwrapped) || isZodObject(unwrapped) || isZodDate(unwrapped)) return "TEXT";
  if (isZodNumber(unwrapped)) return "NUMERIC";
  if (isZodBoolean(unwrapped)) return "INTEGER";
  if (unwrapped._def.typeName === "ZodBigInt") return "TEXT"; // Handle ZodBigInt
  if (isZodDefault(zodType)) return getSqliteType(zodType._def.innerType);
  console.warn(`Unsupported Zod type: ${zodType._def.typeName}`);
  return "TEXT";
}

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
  const fkConstraint = isOutputTable ? `, FOREIGN KEY(id) REFERENCES ${tableName.replace('output_', 'input_')}(id) ON DELETE CASCADE` : "";
  return `CREATE TABLE IF NOT EXISTS ${tableName} (${columns.join(", ")}${fkConstraint})`;
}

async function createAutomaticIndexes(dbInstance: Database, tblName: string, schema: z.ZodObject<any>): Promise<void> {
  if (!dbInstance || dbInstance.closed) return;
  try {
    const tableInfo: { name: string }[] = dbInstance.query(`PRAGMA table_info(${tblName})`).all() as any;
    for (const column of tableInfo) {
      if (column.name === "id" || !schema.shape[column.name]) continue;
      const zodType = schema.shape[column.name];
      const sqlType = getSqliteType(zodType);
      if (sqlType === "NUMERIC" || sqlType === "INTEGER" || isZodEnum(zodType) || isZodDate(zodType)) {
        const indexName = `idx_${tblName}_${column.name}`;
        try {
          dbInstance.query(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${tblName}(${column.name})`).run();
        } catch (error: any) {
          console.warn(`Failed index ${indexName}:`, error?.message);
        }
      }
    }
  } catch (error: any) {
    if (!error.message.includes("no such table")) {
      console.error(`Error table info ${tblName}:`, error?.message);
    }
  }
}

function processValueForStorage(value: any, zodType: z.ZodType): any {
  if (value === undefined || value === null) return null;
  const unwrapped = zodType.unwrap ? zodType.unwrap() : zodType;
  if (value instanceof Date) return value.toISOString();
  if (isZodBoolean(unwrapped)) return value ? 1 : 0;
  if (isZodArray(unwrapped) || isZodObject(unwrapped)) return JSON.stringify(value);
  if (isZodNumber(unwrapped) && typeof value === 'number' && !Number.isFinite(value)) return null;
  if (unwrapped._def.typeName === "ZodBigInt") return value.toString(); // Handle ZodBigInt
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

function deserializeValue(value: any, zodType: z.ZodType): any {
  if (value === null) return null;
  const unwrapped = zodType.unwrap ? zodType.unwrap() : zodType;
  try {
    if (isZodDate(unwrapped)) return new Date(value);
    if (isZodBoolean(unwrapped)) return Boolean(Number(value));
    if (isZodArray(unwrapped) || isZodObject(unwrapped)) return typeof value === 'string' ? JSON.parse(value) : value;
    if (isZodNumber(unwrapped)) return Number(value);
    if (unwrapped._def.typeName === "ZodBigInt") return BigInt(value); // Handle ZodBigInt
    if (isZodString(unwrapped) || isZodEnum(unwrapped)) return String(value);
  } catch (e: any) {
    console.error(`Error deserializing value "${value}" type ${unwrapped?._def?.typeName}:`, e?.message);
    return undefined;
  }
  return value;
}

function buildWhereClause(
  filter: { input?: Filter<any>, output?: Filter<any> } | undefined,
  params: any[],
  inputSchema: z.ZodObject<any>,
  outputSchema: z.ZodObject<any>,
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
          const addColumnSql = `ALTER TABLE ${tableName} ADD COLUMN ${key} ${columnType}${nullClause}${defaultClause}`;
          dbInstance.query(addColumnSql).run();
          console.log(`Added column ${key} to ${tableName}`);
        } else {
          console.warn(`Could not determine SQLite type for new column ${key}`);
        }
      }
    }
  } catch (error: any) {
    if (!error.message.includes("no such table")) {
      console.error(`Error ensuring columns for ${tableName}:`, error?.message);
      throw error;
    }
  }
}

function setupTriggers(db: Database, tableNames: TableNames) {
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
}

function stringifySchemaShape(schema: z.ZodObject<any>): string {
  const shape = schema.shape;
  const sortedKeys = Object.keys(shape).sort();
  const shapeRepresentation: Record<string, string> = {};
  for (const key of sortedKeys) {
    shapeRepresentation[key] = shape[key]._def.typeName;
  }
  return JSON.stringify(shapeRepresentation);
}

// Core Database Interface
export interface SatiDbCore<I extends z.ZodObject<any>, O extends z.ZodObject<any>> {
  init: (measureFn?: MeasureFunction) => Promise<void>;
  upsert: (input: z.infer<I>, output: z.infer<O>, measureFn?: MeasureFunction) => Promise<{ id: string }>;
  insert: (input: z.infer<I>, output?: z.infer<O>, measureFn?: MeasureFunction) => Promise<{ id: string, output_ignored?: boolean }>;
  findRaw: (filter?: { input?: Filter<I>, output?: Filter<O> }, measureFn?: MeasureFunction) => Promise<Array<RawDatabaseRecord>>;
  findByIdRaw: (id: string, measureFn?: MeasureFunction) => Promise<RawDatabaseRecord | null>;
  updateOutput: (input: z.infer<I>, output: Partial<z.infer<O>>, measureFn?: MeasureFunction) => Promise<{ id: string }>;
  delete: (input: z.infer<I>, measureFn?: MeasureFunction) => Promise<{ id: string }>;
  query: <T = any>(sql: string, params: any[], measureFn?: MeasureFunction) => Promise<T[]>;
  getInputSchema: () => I;
  getOutputSchema: () => O;
  getName: () => string;
  close: () => void;
  _internal: {
    generateId: (input: z.infer<I>) => string;
    getDbInstance: () => Database;
    getTableNames: () => TableNames;
    isInitialized: () => boolean;
  };
}

// SatiDbCore Implementation
export function useSatiDbCore<I extends z.ZodObject<any>, O extends z.ZodObject<any>>(
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
    metadataTable: `_sati_metadata_${sanitizedName}`,
    schemaTable: `_schema_${sanitizedName}`
  };

  if ('id' in inputSchema.shape || 'id' in outputSchema.shape) {
    throw new Error(`Schemas cannot contain 'id'.`);
  }

  const dir = path.dirname(dbPath);
  try {
    fs.mkdirSync(dir, { recursive: true });
  } catch (error: any) {
    if (error.code !== 'EEXIST') throw error;
  }

  const db = new Database(dbPath);
  try {
    db.exec("PRAGMA journal_mode = WAL;");
  } catch (e: any) {
    console.warn(`Could not enable WAL mode:`, e.message);
  }
  let initialized = false;

  async function storeSchemaDescriptions(measureFn: MeasureFunction = defaultMeasure) {
    await measureFn(async () => {
      db.query(`
        CREATE TABLE IF NOT EXISTS ${tableNames.schemaTable} (
          table_name TEXT NOT NULL,
          column_name TEXT NOT NULL,
          column_type TEXT NOT NULL,
          description TEXT,
          PRIMARY KEY (table_name, column_name)
        )
      `).run();

      const storeDescriptions = (schema: z.ZodObject<any>, tableName: string) => {
        for (const [key, valueDef] of Object.entries(schema.shape)) {
          const description = valueDef._def.description || null;
          const columnType = getSqliteType(valueDef) || 'TEXT';
          db.query(`
            INSERT OR REPLACE INTO ${tableNames.schemaTable} (table_name, column_name, column_type, description)
            VALUES (?, ?, ?, ?)
          `).run(tableName, key, columnType, description);
        }
      };

      storeDescriptions(inputSchema, tableNames.inputTable);
      storeDescriptions(outputSchema, tableNames.outputTable);
    }, `db_store_schema_${baseName}`);
  }

  const coreApi: SatiDbCore<I, O> = {
    async init(measureFn: MeasureFunction = defaultMeasure): Promise<void> {
      await measureFn(async () => {
        if (initialized || !db || db.closed) return;
        db.transaction(() => {
          db.query(createTableSql(inputSchema, tableNames.inputTable, false)).run();
          db.query(createTableSql(outputSchema, tableNames.outputTable, true)).run();
          db.query(`CREATE TABLE IF NOT EXISTS ${tableNames.metadataTable} (key TEXT PRIMARY KEY, value TEXT NOT NULL)`).run();

          const currentInputShapeStr = stringifySchemaShape(inputSchema);
          const currentOutputShapeStr = stringifySchemaShape(outputSchema);
          const storedInputShape = db.query<{ value: string }>(`SELECT value FROM ${tableNames.metadataTable} WHERE key = 'input_schema_shape'`).get()?.value;
          const storedOutputShape = db.query<{ value: string }>(`SELECT value FROM ${tableNames.metadataTable} WHERE key = 'output_schema_shape'`).get()?.value;

          if (storedInputShape === undefined || storedOutputShape === undefined) {
            db.query(`INSERT OR REPLACE INTO ${tableNames.metadataTable} (key, value) VALUES (?, ?)`).run('input_schema_shape', currentInputShapeStr);
            db.query(`INSERT OR REPLACE INTO ${tableNames.metadataTable} (key, value) VALUES (?, ?)`).run('output_schema_shape', currentOutputShapeStr);
          } else {
            if (storedInputShape !== currentInputShapeStr) throw new Error(`Input schema mismatch. Expected: ${storedInputShape}, Got: ${currentInputShapeStr}`);
            if (storedOutputShape !== currentOutputShapeStr) throw new Error(`Output schema mismatch. Expected: ${storedOutputShape}, Got: ${currentOutputShapeStr}`);
          }

          ensureTableColumns(db, tableNames.inputTable, inputSchema, false);
          ensureTableColumns(db, tableNames.outputTable, outputSchema, true);
          createAutomaticIndexes(db, tableNames.inputTable, inputSchema).catch(e => console.error(`Error creating input indexes:`, e));
          createAutomaticIndexes(db, tableNames.outputTable, outputSchema).catch(e => console.error(`Error creating output indexes:`, e));
          setupTriggers(db, tableNames);
          storeSchemaDescriptions().catch(e => console.error(`Error storing schema descriptions:`, e));
        })();
        initialized = true;
      }, `db_init_${baseName}`);
    },

    async insert(input: z.infer<I>, output?: z.infer<O>, measureFn: MeasureFunction = defaultMeasure): Promise<{ id: string, output_ignored?: boolean }> {
      const validatedInput = inputSchema.parse(input);
      const id = generateRecordId(validatedInput, inputSchema);
      return await measureFn(async () => {
        if (!initialized) await coreApi.init();
        let validatedOutput: z.infer<O> | undefined = undefined;
        if (output !== undefined) {
          validatedOutput = outputSchema.parse(output);
        }
        let output_ignored = false;

        db.transaction(() => {
          const processedInput = processObjectForStorage(validatedInput, inputSchema);
          const inputColumns = Object.keys(processedInput);
          if (inputColumns.length === 0 && Object.keys(validatedInput).length > 0) {
            console.warn(`Input object for ID ${id} resulted in zero columns for storage.`);
          }
          const inputPlaceholders = inputColumns.map(() => "?").join(", ");
          db.query(`INSERT INTO ${tableNames.inputTable} (id${inputColumns.length > 0 ? ", " + inputColumns.join(", ") : ""}) VALUES (?${inputPlaceholders.length > 0 ? ", " + inputPlaceholders : ""}) ON CONFLICT(id) DO NOTHING`)
            .run(id, ...Object.values(processedInput));

          if (validatedOutput) {
            const processedOutput = processObjectForStorage(validatedOutput, outputSchema);
            const outputColumns = Object.keys(processedOutput);
            if (outputColumns.length > 0) {
              const outputPlaceholders = outputColumns.map(() => "?").join(", ");
              const result = db.query(`INSERT OR IGNORE INTO ${tableNames.outputTable} (id, ${outputColumns.join(", ")}) VALUES (?, ${outputPlaceholders})`)
                .run(id, ...Object.values(processedOutput));
              if (result.changes === 0) output_ignored = true;
            } else {
              console.warn(`Output object for ID ${id} resulted in zero columns for storage.`);
            }
          }
        })();

        if (output_ignored) console.log(`Output ignored for ID ${id} (already exists or no columns to insert).`);
        return { id, output_ignored };
      }, `db_insert_${baseName}_${id}`);
    },

    async upsert(input: z.infer<I>, output: z.infer<O>, measureFn: MeasureFunction = defaultMeasure): Promise<{ id: string }> {
      const validatedInput = inputSchema.parse(input);
      const id = generateRecordId(validatedInput, inputSchema);
      return await measureFn(async () => {
        if (!initialized) await coreApi.init();
        const validatedOutput = outputSchema.parse(output);

        db.transaction(() => {
          const processedInput = processObjectForStorage(validatedInput, inputSchema);
          const inputColumns = Object.keys(processedInput);
          const inputPlaceholders = inputColumns.map(() => "?").join(", ");
          db.query(`INSERT INTO ${tableNames.inputTable} (id${inputColumns.length > 0 ? ", " + inputColumns.join(", ") : ""}) VALUES (?${inputPlaceholders.length > 0 ? ", " + inputPlaceholders : ""}) ON CONFLICT(id) DO NOTHING`)
            .run(id, ...Object.values(processedInput));

          const processedOutput = processObjectForStorage(validatedOutput, outputSchema);
          const outputColumns = Object.keys(processedOutput);
          if (outputColumns.length > 0) {
            const outputPlaceholders = outputColumns.map(() => "?").join(", ");
            db.query(`INSERT OR REPLACE INTO ${tableNames.outputTable} (id, ${outputColumns.join(", ")}) VALUES (?, ${outputPlaceholders})`)
              .run(id, ...Object.values(processedOutput));
          } else {
            db.query(`DELETE FROM ${tableNames.outputTable} WHERE id = ?`).run(id);
            console.warn(`Upsert: Output object for ID ${id} resulted in zero columns; existing output (if any) removed.`);
          }
        })();
        return { id };
      }, `db_upsert_${baseName}_${id}`);
    },

    async findRaw(filter?: { input?: Filter<I>, output?: Filter<O> }, measureFn: MeasureFunction = defaultMeasure): Promise<Array<RawDatabaseRecord>> {
      return await measureFn(async () => {
        if (!initialized) await coreApi.init();
        const params: any[] = [];
        const inputCols = ['id', ...Object.keys(inputSchema.shape)].map(c => `i."${c}" AS "i_${c}"`).join(', ');
        const outputCols = Object.keys(outputSchema.shape).map(c => `o."${c}" AS "o_${c}"`).join(', ');
        let sql = `SELECT ${inputCols}${outputCols ? ', ' + outputCols : ''} FROM ${tableNames.inputTable} i LEFT JOIN ${tableNames.outputTable} o ON i.id = o.id`;

        const whereClause = buildWhereClause(filter, params, inputSchema, outputSchema, 'i', 'o');
        sql += ` ${whereClause}`;

        if (filter?.output && Object.keys(filter.output).length > 0) {
          sql += (whereClause ? " AND" : " WHERE") + " o.id IS NOT NULL";
        }

        const results = db.query(sql).all(...params);

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
                if (value !== null) {
                  hasNonNullOutputDbValue = true;
                }
              }
            }
          }
          return {
            id: row.i_id,
            input: inputData,
            ...(hasNonNullOutputDbValue && { output: outputData })
          };
        };

        return results.map(row => mapPrefixedRow(row)).filter(record => record !== null) as Array<RawDatabaseRecord>;
      }, `db_find_raw_${baseName}`);
    },

    async findByIdRaw(id: string, measureFn: MeasureFunction = defaultMeasure): Promise<RawDatabaseRecord | null> {
      return await measureFn(async () => {
        if (!initialized) await coreApi.init();
        const inputCols = ['id', ...Object.keys(inputSchema.shape)].map(c => `i."${c}" AS "i_${c}"`).join(', ');
        const outputCols = Object.keys(outputSchema.shape).map(c => `o."${c}" AS "o_${c}"`).join(', ');
        const sql = `SELECT ${inputCols}${outputCols ? ', ' + outputCols : ''} FROM ${tableNames.inputTable} i LEFT JOIN ${tableNames.outputTable} o ON i.id = o.id WHERE i.id = ?`;
        const row = db.query(sql).get(id);

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
                if (value !== null) hasNonNullOutputDbValue = true;
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
      }, `db_find_by_id_${baseName}_${id}`);
    },

    async updateOutput(input: z.infer<I>, output: Partial<z.infer<O>>, measureFn: MeasureFunction = defaultMeasure): Promise<{ id: string }> {
      return await measureFn(async () => {
        if (!initialized) await coreApi.init();
        const validatedInput = inputSchema.parse(input);
        const id = generateRecordId(validatedInput, inputSchema);
        const validatedOutputUpdate = outputSchema.partial().parse(output);
        if (Object.keys(validatedOutputUpdate).length === 0) {
          console.log(`updateOutput called with empty output object for ID ${id}. No changes made.`);
          return { id };
        }

        const inputExists = db.query(`SELECT 1 FROM ${tableNames.inputTable} WHERE id = ?`).get(id);
        if (!inputExists) {
          throw new Error(`UpdateOutput failed: Input ID ${id} not found.`);
        }

        const existingOutputRow = db.query(`SELECT 1 FROM ${tableNames.outputTable} WHERE id = ?`).get(id);
        if (existingOutputRow) {
          throw new Error(`UpdateOutput failed: Output already exists for ID ${id}. Use upsert if overwrite is intended.`);
        }

        let fullOutputData: z.infer<O>;
        try {
          fullOutputData = outputSchema.parse(validatedOutputUpdate);
        } catch (parseError: any) {
          console.error(`Failed to parse partial output for ID ${id}:`, parseError.errors);
          throw new Error(`UpdateOutput failed: Provided partial output is insufficient. ${parseError.message}`);
        }

        const processedOutput = processObjectForStorage(fullOutputData, outputSchema);
        const columns = Object.keys(processedOutput);

        if (columns.length > 0) {
          const placeholders = columns.map(() => "?").join(", ");
          db.query(`INSERT OR IGNORE INTO ${tableNames.outputTable} (id, ${columns.join(", ")}) VALUES (?, ${placeholders})`)
            .run(id, ...Object.values(processedOutput));
        } else {
          console.warn(`updateOutput for ID ${id} resulted in zero columns. No output row inserted.`);
        }
        return { id };
      }, `db_update_output_${baseName}_${id}`);
    },

    async delete(input: z.infer<I>, measureFn: MeasureFunction = defaultMeasure): Promise<{ id: string }> {
      const validatedInput = inputSchema.parse(input);
      const id = generateRecordId(validatedInput, inputSchema);
      return await measureFn(async () => {
        if (!initialized) await coreApi.init();
        const result = db.query(`DELETE FROM ${tableNames.inputTable} WHERE id = ?`).run(id);
        if (result.changes === 0) {
          throw new Error(`Delete input failed: Record ID ${id} not found.`);
        }
        const result2 = db.query(`DELETE FROM ${tableNames.outputTable} WHERE id = ?`).run(id);
        if (result2.changes === 0) {
          throw new Error(`Delete output failed: Record ID ${id} not found.`);
        }
        return { id };
      }, `db_delete_${baseName}_${id}`);
    },

    async query<T = any>(sql: string, params: any[], measureFn: MeasureFunction = defaultMeasure): Promise<T[]> {
      return await measureFn(async () => {
        if (!initialized) await coreApi.init();
        try {
          const statement = db.prepare(sql);
          if (statement.paramsCount > 0) {
            return statement.all(...params) as T[];
          } else {
            return statement.all() as T[];
          }
        } catch (error: any) {
          console.error(`Error executing SQL: "${sql}" with params: ${JSON.stringify(params)}`, error?.message);
          throw error;
        }
      }, `db_query_${baseName}`);
    },

    getInputSchema: () => inputSchema,
    getOutputSchema: () => outputSchema,
    getName: () => baseName,

    close(): void {
      if (db && !db.closed) {
        try {
          db.close();
        } catch (e: any) {
          console.error(`Error closing DB:`, e?.message);
        }
      }
      initialized = false;
      console.log(`Closed.`);
    },

    _internal: {
      generateId: (input: z.infer<I>) => generateRecordId(input, inputSchema),
      getDbInstance: () => db,
      getTableNames: () => tableNames,
      isInitialized: () => initialized,
    }
  };
  return coreApi;
}

// useDatabase Wrapper for tryHelius.ts Compatibility
export function useDatabase<I extends z.ZodObject<any>, O extends z.ZodObject<any>>(
  dbPath: string,
  inputSchema: I,
  outputSchema: O
): [(input: z.infer<I>, output: z.infer<O> | null, measureFn?: MeasureFunction) => Promise<void>, (query: Partial<z.infer<I>>, measureFn?: MeasureFunction) => Promise<(z.infer<O> & z.infer<I>)[]>] {
  const coreDb = useSatiDbCore(dbPath, inputSchema, outputSchema);

  const save = async (input: z.infer<I>, output: z.infer<O> | null, measureFn: MeasureFunction = defaultMeasure) => {
    await measureFn(async () => {
      await coreDb.init();
      if (output === null) {
        try {
          await coreDb.delete(input, measureFn);
        } catch (error: any) {
          if (!error.message.includes("not found")) {
            console.error(`Error deleting record:`, error.message);
          }
        }
      } else {
        await coreDb.upsert(input, output, measureFn);
      }
    }, `db_save_${path.basename(dbPath, path.extname(dbPath))}_${generateRecordId(input, inputSchema)}`);
  };

  const find = async (query: Partial<z.infer<I>>, measureFn: MeasureFunction = defaultMeasure) => {
    return await measureFn(async () => {
      await coreDb.init();
      const inputFilter: Filter<I> = {};
      for (const [key, value] of Object.entries(query)) {
        if (value !== undefined) {
          inputFilter[key as keyof z.infer<I>] = value as any;
        }
      }
      const results = await coreDb.findRaw({ input: inputFilter }, measureFn);
      return results
        .filter(record => record.input && Object.keys(record.input).length > 0)
        .map(record => {
          const merged: z.infer<O> & z.infer<I> = { ...record.output, ...record.input } as any;
          return merged;
        });
    }, `db_find_${path.basename(dbPath, path.extname(dbPath))}`);
  };

  return [save, find];
}