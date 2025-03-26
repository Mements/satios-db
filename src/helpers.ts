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


export async function measure<T>(
  fn: (measure: typeof measure) => Promise<T>,
  action: string,
  context: MeasureContext = {}
): Promise<T> {
  const start = performance.now();
  const level = context.level || 0;
  let indent = "=".repeat(level);
  const requestId = context.requestId;
  let logPrefix = requestId ? `[${requestId}] ${indent}>` : `${indent}>`;

  try {
    // Log the start of the action with "..."
    indent = ">".repeat(level);
    logPrefix = requestId ? `[${requestId}] ${indent}$` : `${indent}$`;
    console.log(`${logPrefix} ${action}...`);
    
    const result = await fn((nestedFn, nestedAction) =>
      measure(nestedFn, nestedAction, {
        requestId: requestId ? `${requestId}` : undefined,
        level: level + 1,
        parentAction: action
      })
    );
    
    const duration = performance.now() - start;
    indent = "<".repeat(level);
    logPrefix = requestId ? `[${requestId}] ${indent}$` : `${indent}$`;
    console.log(`${logPrefix} ${action} ✓ ${duration.toFixed(2)}ms`);
    return result;
  } catch (error) {
    const duration = performance.now() - start;
    // Log failure with "✗", duration, and error
    console.log(`${logPrefix} ${action} ✗ ${duration.toFixed(2)}ms`, error);
    throw new Error(`${action} failed: ${error}`);
  }
}