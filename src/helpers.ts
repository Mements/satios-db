import chalk from "chalk";
import dedent from "dedent";
import { z } from "zod";
import { OUTPUT_DATA_WRAPPER } from "./constants";
import { dezerialize, zerialize, zerialize } from "zodex";

export const verboseLog = (category: string, action: string, details?: any) => {
  const isVerbose = process.env.VERBOSE_MODE !== undefined
    ? process.env.VERBOSE_MODE === "true"
    : import.meta.main;

  if (isVerbose) {
    const categoryStr = chalk.blue(`[${category}]`);
    const actionStr = chalk.green(action);

    let detailsStr = "";
    if (details) {
      if (typeof details === "string" && details.includes("ms")) {
        detailsStr = chalk.gray(` (${details})`);
      } else if (details instanceof Error) {
        console.error(details);
        detailsStr = `${details}`;
      } else {
        const indent = " ".repeat(category.length + 3);
        const formattedDetails = typeof details === "string"
          ? details
          : JSON.stringify(details, null, 2)
              .split("\n")
              .join(`\n${indent}`);
        detailsStr = `\n${indent}${chalk.gray(formattedDetails)}`;
      }
    }

    const message = `${categoryStr} ${actionStr}${detailsStr}`;

    if (action?.startsWith("Starting") || action?.startsWith("Completed")) {
      console.log("");
    }

    console.log(message);
  }
};

export type MeasureContext = {
  requestId?: string;
  level?: number;
  parentAction?: string;
};

export type MeasureFunction = <T>(
    fn: (measure: MeasureFunction) => Promise<T>,
    action: string,
) => Promise<T>;


export async function measure<T>(
  fn: (measure: MeasureFunction) => Promise<T>,
  action: string,
  context: MeasureContext = {}
): Promise<T> {
  const start = performance.now();
  const level = context.level || 0;
  const requestId = context.requestId;
  const parentInfo = context.parentAction ? ` (parent: ${context.parentAction})` : '';

  // Log start
  let startIndent = "  ".repeat(level);
  let startPrefix = requestId ? `[${requestId}] ${startIndent}┌─` : `${startIndent}┌─`;
  console.log(`${startPrefix} ${action}${parentInfo}...`);

  try {
    const nestedMeasure: MeasureFunction = (nestedFn, nestedAction) =>
      measure(nestedFn, nestedAction, {
        requestId: requestId, // Pass down the same requestId
        level: level + 1,
        parentAction: action
      });

    const result = await fn(nestedMeasure);

    const duration = performance.now() - start;
    // Log success
    let endIndent = "  ".repeat(level);
    let endPrefix = requestId ? `[${requestId}] ${endIndent}└─` : `${endIndent}└─`;
    console.log(`${endPrefix} ${action} ✓ (${duration.toFixed(2)}ms)`);
    return result;
  } catch (error: any) {
    const duration = performance.now() - start;
    // Log failure
    let errorIndent = "  ".repeat(level);
    let errorPrefix = requestId ? `[${requestId}] ${errorIndent}└─` : `${errorIndent}└─`;
    console.error(`${errorPrefix} ${action} ✗ (${duration.toFixed(2)}ms) Error: ${error.message}`);
    // Rethrow preserving stack trace if possible, otherwise wrap
    // throw error instanceof Error ? error : new Error(`${action} failed: ${error}`);
    // Let's wrap it consistently for clarity in this context
    throw new Error(`${action} failed: ${error.message || error}`);
  }
}