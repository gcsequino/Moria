import { lstatSync, readdirSync, existsSync } from 'node:fs';
import { dirname, isAbsolute, join, relative, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import * as ts from 'typescript/unstable/ast';
import { API } from 'typescript/unstable/sync';
import {
  protectedClasses,
  protectedFunctions,
  protectedModules,
  objectServiceMethods,
  objectDispatchers,
  httpListeners,
  httpClientImports,
} from './protected-boundaries.js';

const production = ['apps', 'packages', 'services', 'workers'];
const slash = (path: string) => path.replaceAll('\\', '/');

export function architectureViolations(root: string): string[] {
  root = resolve(root);
  const files: string[] = [];
  const violations: string[] = [];
  const walk = (directory: string) => {
    for (const entry of readdirSync(directory)) {
      const path = join(directory, entry);
      const stat = lstatSync(path);
      if (stat.isSymbolicLink())
        throw new Error(`Production source symlink: ${slash(relative(root, path))}`);
      if (stat.isDirectory()) {
        if (
          entry !== 'node_modules' &&
          !['apps/management/dist', 'apps/snotel/dist'].includes(slash(relative(root, path)))
        )
          walk(path);
      } else if (/\.(?:[cm]?[jt]s|[jt]sx)$/.test(entry)) files.push(path);
    }
  };
  for (const name of production) if (existsSync(join(root, name))) walk(join(root, name));
  const api = new API({ cwd: root });
  try {
    const snapshot = api.updateSnapshot({ openProjects: [join(root, 'tsconfig.json')] });
    const project = snapshot.getProject(join(root, 'tsconfig.json'));
    if (!project) throw new Error('Architecture project unavailable');
    const { program, checker } = project;
    const name = (path: string) => slash(relative(root, path));
    const report = (source: ts.SourceFile, node: ts.Node, reason: string) => {
      const line = source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1;
      violations.push(`${name(source.fileName)}:${line}: ${reason}`);
    };
    const moduleCheck = (
      source: ts.SourceFile,
      node: ts.Node,
      specifier: string,
      clientImport = false,
    ) => {
      if (
        Object.hasOwn(protectedModules, specifier) &&
        !protectedModules[specifier].includes(name(source.fileName)) &&
        !clientImport
      )
        report(source, node, `Unregistered runtime module ${specifier}`);
      if (specifier.startsWith('.') || isAbsolute(specifier) || specifier.startsWith('file:')) {
        const path = specifier.startsWith('file:')
          ? fileURLToPath(specifier)
          : resolve(dirname(source.fileName), specifier);
        const candidates = [
          path,
          path.replace(/\.js$/, '.ts'),
          path.replace(/\.mjs$/, '.mts'),
          path.replace(/\.cjs$/, '.cts'),
        ];
        const functionOwners = Object.entries(protectedFunctions).filter(([key]) =>
          candidates.some((candidate) => name(candidate) === key.split('#')[0]),
        );
        if (
          functionOwners.length &&
          !functionOwners.some(([, owners]) => owners.includes(name(source.fileName)))
        )
          report(source, node, 'Unregistered boundary loader');
        for (const [key, owners] of Object.entries(protectedClasses)) {
          if (
            candidates.some((candidate) => name(candidate) === key.split('#')[0]) &&
            !owners.includes(name(source.fileName))
          )
            report(source, node, `Unregistered capability loader for ${key}`);
        }
      }
    };
    const classKey = (declaration: ts.Node | undefined): string | undefined => {
      for (let node = declaration; node; node = node.parent) {
        if (ts.isClassDeclaration(node) && node.name)
          return `${name(node.getSourceFile().fileName)}#${node.name.text}`;
      }
    };
    const guarded = (
      source: ts.SourceFile,
      node: ts.Node,
      key: string | undefined,
      owners: Record<string, readonly string[]>,
    ) => {
      if (key && owners[key] && !owners[key].includes(name(source.fileName)))
        report(source, node, `Protected capability ${key}; use its registered boundary`);
    };
    for (const file of files) {
      const source = program.getSourceFile(file);
      if (!source) throw new Error(`Cannot parse ${name(file)}`);
      const visitor = (node: ts.Node) => {
        if (ts.isCallExpression(node) || ts.isNewExpression(node)) {
          const signature = checker.getResolvedSignature(node);
          const declaration = signature?.declaration?.resolve();
          guarded(source, node, classKey(declaration), protectedClasses);
          if (declaration && ts.isFunctionDeclaration(declaration) && declaration.name) {
            guarded(
              source,
              node,
              `${name(declaration.getSourceFile().fileName)}#${declaration.name.text}`,
              protectedFunctions,
            );
          }
          if (
            ts.isCallExpression(node) &&
            (node.expression.kind === ts.SyntaxKind.ImportKeyword ||
              (ts.isIdentifier(node.expression) && node.expression.text === 'require'))
          ) {
            if (node.arguments.length !== 1 || !ts.isStringLiteral(node.arguments[0]))
              report(source, node, 'Computed capability imports are unsupported in production');
            else moduleCheck(source, node, node.arguments[0].text);
          }
        }
        if (ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node)) {
          if (name(file) !== 'services/http/src/server.ts') {
            const type = checker.getTypeAtLocation(node.expression);
            const declarations = type?.getSymbol()?.declarations ?? [];
            const httpServer = declarations.some((handle) => {
              const declaration = handle.resolve();
              return (
                !!declaration &&
                (ts.isClassDeclaration(declaration) || ts.isInterfaceDeclaration(declaration)) &&
                declaration.name?.text === 'Server' &&
                /(?:^|\/)https?\.d\.ts$/.test(slash(declaration.getSourceFile().fileName))
              );
            });
            const member = ts.isPropertyAccessExpression(node)
              ? node.name.text
              : ts.isStringLiteral(node.argumentExpression)
                ? node.argumentExpression.text
                : '';
            if (
              httpServer &&
              [
                'requestTimeout',
                'headersTimeout',
                'maxHeadersCount',
                'maxRequestsPerSocket',
                'timeout',
                'setTimeout',
              ].includes(member)
            )
              report(
                source,
                node,
                'HTTP timeout and limit policy belongs to the transport boundary',
              );
            if (
              httpServer &&
              [
                'on',
                'once',
                'addListener',
                'prependListener',
                'prependOnceListener',
                'emit',
                'removeListener',
                'off',
                'removeAllListeners',
                'listeners',
                'rawListeners',
              ].includes(member)
            ) {
              const call = node.parent;
              if (
                !ts.isCallExpression(call) ||
                call.expression !== node ||
                !call.arguments[0] ||
                !ts.isStringLiteral(call.arguments[0]) ||
                !['error', 'close', 'listening'].includes(call.arguments[0].text)
              )
                report(source, node, 'HTTP request listeners belong to the transport boundary');
            }
          }
          const symbol = checker.getSymbolAtLocation(
            ts.isPropertyAccessExpression(node) ? node.name : node.argumentExpression,
          );
          for (const declaration of symbol?.declarations ?? [])
            guarded(source, node, classKey(declaration.resolve()), protectedClasses);
        }
        if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) {
          const specifier = node.moduleSpecifier;
          if (specifier && ts.isStringLiteral(specifier)) {
            const clause = ts.isImportDeclaration(node) ? node.importClause : undefined;
            const typeOnly = ts.isImportDeclaration(node)
              ? clause?.phaseModifier === ts.SyntaxKind.TypeKeyword ||
                (!clause?.name &&
                  clause?.namedBindings &&
                  ts.isNamedImports(clause.namedBindings) &&
                  clause.namedBindings.elements.length > 0 &&
                  clause.namedBindings.elements.every((element) => element.isTypeOnly))
              : node.isTypeOnly;
            if (!typeOnly) {
              const allowed = httpClientImports[name(file)]?.[specifier.text];
              const imports = clause?.namedBindings;
              const clientImport =
                !!allowed &&
                !clause?.name &&
                !!imports &&
                ts.isNamedImports(imports) &&
                imports.elements.every(
                  (element) =>
                    element.isTypeOnly ||
                    allowed.includes((element.propertyName ?? element.name).text),
                );
              moduleCheck(source, node, specifier.text, clientImport);
            }
            const target = checker
              .getSymbolAtLocation(specifier)
              ?.declarations[0]?.resolve()
              ?.getSourceFile().fileName;
            if (target) {
              for (const [key, owners] of Object.entries(protectedClasses)) {
                if (name(target) === key.split('#')[0] && !owners.includes(name(file))) {
                  // Type-only imports do not provide executable access to persistence.
                  if (!typeOnly) report(source, node, `Unregistered import/re-export of ${key}`);
                }
              }
            }
          }
        }
        if (
          ts.isClassDeclaration(node) &&
          node.name?.text === 'ObjectService' &&
          name(file) === 'services/ontology/src/service.ts'
        ) {
          const methods = node.members
            .filter(ts.isMethodDeclaration)
            .filter(
              (method) =>
                !method.modifiers?.some(
                  (modifier) => modifier.kind === ts.SyntaxKind.PrivateKeyword,
                ),
            );
          const names = methods.map((method) => method.name.getText(source)).sort();
          if (JSON.stringify(names) !== JSON.stringify([...objectServiceMethods].sort()))
            report(
              source,
              node,
              'Public object operations must be registered in the contract suite',
            );
        }
        node.forEachChild(visitor);
      };
      visitor(source);
    }
    for (const [dispatcher, tests] of Object.entries({ ...objectDispatchers, ...httpListeners })) {
      if (!files.some((file) => name(file) === dispatcher))
        violations.push(`Missing registered dispatcher ${dispatcher}`);
      for (const test of tests)
        if (!existsSync(join(root, test)))
          violations.push(`Missing contract test ${test} for ${dispatcher}`);
    }
    return [...new Set(violations)].sort();
  } finally {
    api.close();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const violations = architectureViolations(process.cwd());
  if (violations.length) {
    console.error(violations.join('\n'));
    process.exitCode = 1;
  } else console.log('Protected boundary architecture checks passed.');
}
