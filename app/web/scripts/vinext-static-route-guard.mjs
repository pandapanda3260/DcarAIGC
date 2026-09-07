import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import ts from "typescript";

export const STATIC_NAVIGATION_ROUTES = Object.freeze([
  "/overview", "/contents", "/accounts", "/selling-points", "/spu-audience", "/tasks", "/users",
]);

const fail = (file, reason) => { throw new Error(`[dcar navigation guard] ${file}: ${reason}`); };
const parse = (file) => ts.createSourceFile(file, readFileSync(file, "utf8"), ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
const isClient = (file) => {
  const first = parse(file).statements[0];
  return ts.isExpressionStatement(first) && ts.isStringLiteral(first.expression) && first.expression.text === "use client";
};

function literal(node) {
  if (ts.isStringLiteral(node) || ts.isNumericLiteral(node) || [ts.SyntaxKind.TrueKeyword, ts.SyntaxKind.FalseKeyword, ts.SyntaxKind.NullKeyword].includes(node.kind)) return true;
  if (ts.isArrayLiteralExpression(node)) return node.elements.every(literal);
  if (ts.isObjectLiteralExpression(node)) return node.properties.every((property) => ts.isPropertyAssignment(property) && (ts.isIdentifier(property.name) || ts.isStringLiteral(property.name)) && literal(property.initializer));
  return false;
}

// Deliberately small grammar: imports of client components, literal metadata,
// and a synchronous default function returning only JSX/children. Expanding a
// server wrapper requires an explicit review rather than silently unsafe reuse.
function verifyWrapper(file, kind) {
  const source = parse(file);
  const components = new Set();
  let defaultFunction = null;
  for (const statement of source.statements) {
    if (ts.isImportDeclaration(statement)) {
      if (statement.importClause?.isTypeOnly) continue;
      const moduleName = statement.moduleSpecifier.text;
      if (!statement.importClause && moduleName.endsWith(".css")) continue;
      if (!moduleName.startsWith(".") || !statement.importClause?.name || statement.importClause.namedBindings) fail(file, "runtime imports must be default client components");
      const imported = ["", ".tsx", ".ts", "/index.tsx"].map((suffix) => resolve(dirname(file), moduleName + suffix)).find((candidate) => existsSync(candidate) && /\.[tj]sx?$/.test(candidate));
      if (!imported || !isClient(imported)) fail(file, `${moduleName} is not a client boundary`);
      components.add(statement.importClause.name.text);
      continue;
    }
    if (ts.isVariableStatement(statement)) {
      const declarations = statement.declarationList.declarations;
      if (!statement.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword) || declarations.length !== 1 || declarations[0].name.getText(source) !== "metadata" || !declarations[0].initializer || !literal(declarations[0].initializer)) fail(file, "only literal metadata is allowed outside the wrapper");
      continue;
    }
    if (ts.isFunctionDeclaration(statement) && statement.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.DefaultKeyword) && !statement.modifiers.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword) && !defaultFunction) {
      defaultFunction = statement;
      continue;
    }
    fail(file, "unknown server statement");
  }
  if (!defaultFunction?.body || defaultFunction.body.statements.length !== 1 || !ts.isReturnStatement(defaultFunction.body.statements[0])) fail(file, "wrapper must contain only a JSX return");
  if (kind === "layout") {
    const parameters = defaultFunction.parameters;
    if (parameters.length !== 1 || !ts.isObjectBindingPattern(parameters[0].name) || parameters[0].initializer || parameters[0].name.elements.length !== 1 || parameters[0].name.elements[0].getText(source) !== "children") fail(file, "layout can receive children only");
  } else if (defaultFunction.parameters.length) fail(file, "page/loading cannot receive server params");

  function jsx(node) {
    if (ts.isParenthesizedExpression(node)) return jsx(node.expression);
    if (ts.isJsxText(node)) return;
    if (ts.isJsxExpression(node)) {
      if (kind === "layout" && node.expression && ts.isIdentifier(node.expression) && node.expression.text === "children") return;
      fail(file, "JSX expressions other than layout children are forbidden");
    }
    if (ts.isJsxFragment(node)) { node.children.forEach(jsx); return; }
    const opening = ts.isJsxElement(node) ? node.openingElement : ts.isJsxSelfClosingElement(node) ? node : null;
    if (!opening || !ts.isIdentifier(opening.tagName) || !(components.has(opening.tagName.text) || (kind === "layout" && ["html", "body"].includes(opening.tagName.text)))) fail(file, "unknown JSX element");
    for (const attribute of opening.attributes.properties) {
      if (!ts.isJsxAttribute(attribute) || (attribute.initializer && !ts.isStringLiteral(attribute.initializer))) fail(file, "wrapper attributes must be static strings");
    }
    if (ts.isJsxElement(node)) node.children.forEach(jsx);
  }
  jsx(defaultFunction.body.statements[0].expression);
}

export function verifyStaticNavigationRoutes(root) {
  const app = join(root, "app");
  function scan(directory) {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      if (entry.name.startsWith("@") || entry.name.startsWith("(")) fail(directory, "parallel/intercepted/group routes require a cache review");
      scan(join(directory, entry.name));
    }
  }
  scan(app);
  for (const prefix of [root, join(root, "src"), app]) {
    for (const name of ["middleware", "proxy"]) for (const extension of ["ts", "tsx", "js", "mjs"]) {
      if (existsSync(join(prefix, `${name}.${extension}`))) fail(prefix, `${name} requires a cache review`);
    }
  }
  verifyWrapper(join(app, "layout.tsx"), "layout");
  for (const route of STATIC_NAVIGATION_ROUTES) verifyWrapper(join(app, route, "page.tsx"), "page");
  for (const directory of [app, ...STATIC_NAVIGATION_ROUTES.map((route) => join(app, route))]) {
    if (directory !== app && existsSync(join(directory, "layout.tsx"))) verifyWrapper(join(directory, "layout.tsx"), "layout");
    if (existsSync(join(directory, "loading.tsx"))) verifyWrapper(join(directory, "loading.tsx"), "loading");
    for (const name of ["error.tsx", "global-error.tsx"]) if (existsSync(join(directory, name)) && !isClient(join(directory, name))) fail(directory, `${name} must be a client boundary`);
    for (const name of ["template.tsx", "default.tsx", "layout.ts", "page.ts", "loading.ts"]) if (existsSync(join(directory, name))) fail(directory, `${name} requires a cache review`);
  }
  return [...STATIC_NAVIGATION_ROUTES];
}
