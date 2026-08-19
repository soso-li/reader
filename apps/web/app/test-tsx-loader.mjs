import { existsSync, readFileSync } from "node:fs";
import { extname } from "node:path";
import { fileURLToPath } from "node:url";
import { registerHooks } from "node:module";

import ts from "typescript";

const navigationStubUrl = new URL("./test-next-navigation.mjs", import.meta.url).href;
const headersStubUrl = new URL("./test-next-headers.mjs", import.meta.url).href;
const linkStubUrl = new URL("./test-next-link.mjs", import.meta.url).href;

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === "next/navigation") {
      return { shortCircuit: true, url: navigationStubUrl };
    }
    if (specifier === "next/headers") {
      return { shortCircuit: true, url: headersStubUrl };
    }
    if (specifier === "next/link") {
      return { shortCircuit: true, url: linkStubUrl };
    }
    if (specifier.startsWith(".") && context.parentURL?.startsWith("file:")) {
      const unresolved = new URL(specifier, context.parentURL);
      if (!extname(unresolved.pathname)) {
        for (const suffix of [".ts", ".tsx"]) {
          const candidate = new URL(`${specifier}${suffix}`, context.parentURL);
          if (existsSync(fileURLToPath(candidate))) {
            return nextResolve(candidate.href, context);
          }
        }
      }
    }
    return nextResolve(specifier, context);
  },
  load(url, context, nextLoad) {
    if (!url.startsWith("file:") || !url.endsWith(".tsx")) {
      return nextLoad(url, context);
    }
    const source = readFileSync(fileURLToPath(url), "utf8");
    const output = ts.transpileModule(source, {
      compilerOptions: {
        jsx: ts.JsxEmit.ReactJSX,
        module: ts.ModuleKind.ESNext,
        target: ts.ScriptTarget.ES2022
      },
      fileName: fileURLToPath(url)
    }).outputText;
    return { format: "module", shortCircuit: true, source: output };
  }
});
