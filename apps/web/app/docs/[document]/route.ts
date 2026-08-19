import { readFile } from "node:fs/promises";
import { join, resolve } from "node:path";

const PROJECT_DOCUMENTS = new Set(["PRODUCT.md", "ARCHITECTURE.md"]);

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ document: string }> }
) {
  const { document } = await params;
  if (!PROJECT_DOCUMENTS.has(document)) return new Response("Not found", { status: 404 });

  for (const directory of projectDocumentDirectories()) {
    try {
      const markdown = await readFile(join(directory, document), "utf8");
      return new Response(markdown, {
        headers: {
          "Cache-Control": "no-store",
          "Content-Type": "text/markdown; charset=utf-8"
        }
      });
    } catch {
      continue;
    }
  }
  return new Response("Not found", { status: 404 });
}

function projectDocumentDirectories() {
  return [
    process.env.READER_DOCS_DIR?.trim(),
    join(process.cwd(), "project-docs"),
    resolve(process.cwd(), "../../docs")
  ].filter((directory): directory is string => Boolean(directory));
}
