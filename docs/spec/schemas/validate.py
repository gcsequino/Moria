"""Validate this review package, not platform authorization or runtime behavior.

Usage: python schemas/validate.py (install schemas/requirements.txt first).
"""

import argparse
import json
import re
from pathlib import Path
from copy import deepcopy
from importlib.metadata import version
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parents[1]


def load(path):
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


resources_schema = load("schemas/resources.schema.json")
manifest_schema = load("schemas/manifest.schema.json")
values_schema = load("schemas/values.schema.json")
registry = Registry().with_resources(
    [
        (s["$id"], Resource.from_contents(s))
        for s in (resources_schema, manifest_schema, values_schema)
    ]
)
assert {"uri", "uuid"} <= set(
    FormatChecker.checkers
), "Install jsonschema[format] so URI/UUID checks cannot silently be skipped."
for schema in (resources_schema, manifest_schema, values_schema):
    Draft202012Validator.check_schema(schema)
rv = Draft202012Validator(resources_schema, registry=registry, format_checker=FormatChecker())
mv = Draft202012Validator(manifest_schema, registry=registry, format_checker=FormatChecker())


def semantic_checks(manifest):
    """Small explicit static subset; see README for unimplemented compiler checks."""
    errors = []
    items = {}
    namespaces = set()
    for r in manifest["resources"]:
        key = f"{r['metadata']['namespace']}/{r['kind']}/{r['metadata']['name']}"
        if key in items:
            errors.append("duplicate resource: " + key)
        items[key] = r
        if r["kind"] == "Namespace":
            namespaces.add(r["metadata"]["name"])
            if r["metadata"]["namespace"] != "_project":
                errors.append("namespace must use _project")

    def target(value, kinds):
        if value not in items:
            errors.append("unresolved reference: " + value)
            return None
        found = items[value]
        if found["kind"] not in kinds:
            errors.append("wrong reference kind: " + value)
            return None
        return found

    def expr_context(expression, allowed):
        for key in ("field", "principalAttribute", "parameter"):
            if key in expression and key not in allowed:
                errors.append("invalid expression context: " + key)
        for x in expression.get("args", []):
            expr_context(x, allowed)

    def types(fields):
        for f in fields.values():
            if f["type"] == "decimal" and f["scale"] > f["precision"]:
                errors.append("decimal scale exceeds precision")
            if f["type"] == "array":
                if f["items"]["nullable"]:
                    errors.append("nullable array elements unsupported")
                types({"item": f["items"]})

    graph = {}
    producers = {}
    for key, r in items.items():
        k = r["kind"]
        s = r["spec"]
        m = r["metadata"]
        if k != "Namespace" and m["namespace"] not in namespaces:
            errors.append("unknown namespace")
        for mark in m.get("markings", []):
            target(mark, {"Marking"})
        for name in ("columns", "properties", "inputs", "outputs"):
            if isinstance(s.get(name), dict):
                types(s[name])
        if k == "Dataset":
            for pk in s.get("primaryKey", []):
                if pk not in s["columns"] or s["columns"][pk]["nullable"]:
                    errors.append("invalid dataset primary key")
            if set(s["writeModes"]) & {"upsert", "delete"} and not s.get("primaryKey"):
                errors.append("key required for keyed writes")
        elif k == "Transform":
            for inp in s["inputs"]:
                target(inp, {"Dataset"})
            for out in s["outputs"]:
                target(out, {"Dataset"})
                if out in producers:
                    errors.append("multiple transform producers")
                producers[out] = key
            graph[key] = s["inputs"]
        elif k == "RestrictedView":
            target(s["dataset"], {"Dataset"})
            p = target(s["policy"], {"Policy"})
            if p and p["spec"]["type"] != "row":
                errors.append("restricted view requires row policy")
        elif k == "ObjectType":
            props = s["properties"]
            pk = s["primaryKey"]
            if pk not in props or props[pk] != {"type": "uid", "nullable": False}:
                errors.append("object key must be required uid")
            src = target(s["source"]["resource"], {"Dataset", "RestrictedView"})
            if src and src["kind"] == "RestrictedView":
                src = target(src["spec"]["dataset"], {"Dataset"})
            if set(s["source"]["mapping"]) != set(props):
                errors.append("object mapping must cover properties")
            if src:
                for prop, col in s["source"]["mapping"].items():
                    if col not in src["spec"]["columns"]:
                        errors.append("missing mapped column")
                    elif prop in props and props[prop] != src["spec"]["columns"][col]:
                        errors.append("mapped property type mismatch")
            for field, kind in [("readPolicy", "objectRead"), ("writePolicy", "objectWrite")]:
                if field in s:
                    p = target(s[field], {"Policy"})
                    if p and p["spec"]["type"] != kind:
                        errors.append("wrong object policy kind")
            for prop, policies in s.get("propertyPolicies", {}).items():
                if prop not in props:
                    errors.append("policy on unknown property")
                for op, val in policies.items():
                    p = target(val, {"Policy"})
                    if p and p["spec"]["type"] != (
                        "propertyRead" if op == "read" else "propertyWrite"
                    ):
                        errors.append("wrong property policy kind")
        elif k == "LinkType":
            src = target(s["sourceType"], {"ObjectType"})
            dst = target(s["targetType"], {"ObjectType"})
            if s["mode"] == "mapped":
                if s["cardinality"] == "manyToMany":
                    errors.append("mapped many-to-many unsupported")
                for field, t in [("sourceProperty", src), ("targetProperty", dst)]:
                    if field not in s or (t and s[field] not in t["spec"]["properties"]):
                        errors.append("bad mapped link key")
            elif "sourceProperty" in s or "targetProperty" in s:
                errors.append("action link has mapped fields")
            if "readPolicy" in s:
                target(s["readPolicy"], {"Policy"})
        elif k == "Action":
            target(s["targetType"], {"ObjectType"})
            if "targetParameter" in s and s["targetParameter"] not in s["inputs"]:
                errors.append("missing action target parameter")
            if "targetParameter" not in s and (
                "function" in s or any(e["op"] != "create" for e in s.get("edits", []))
            ):
                errors.append("existing-object action needs target parameter")
            if "function" in s:
                target(s["function"], {"Function"})
            for edit in s.get("edits", []):
                if "targetType" in edit:
                    t = target(edit["targetType"], {"ObjectType"})
                    if t and "property" in edit and edit["property"] not in t["spec"]["properties"]:
                        errors.append("unknown edit property")
                if "linkType" in edit:
                    target(edit["linkType"], {"LinkType"})
        elif k == "Application":
            for scope in s["scopes"]:
                target(scope["target"], set(x["kind"] for x in items.values()))
            if "frontend" in s and s["mode"] != "browser":
                errors.append("frontend requires browser application")
        elif k == "Connection":
            impl = target(s["implementation"], {"ConnectorImplementation"})
            out = target(s["output"], {"Dataset"})
            if out and s["mode"] not in out["spec"]["writeModes"]:
                errors.append("connection mode not allowed by dataset")
            for sec in s["secrets"]:
                target(sec, {"SecretReference"})
            if impl:
                try:
                    Draft202012Validator.check_schema(impl["spec"]["configurationSchema"])
                    Draft202012Validator(impl["spec"]["configurationSchema"]).validate(
                        s["configuration"]
                    )
                except Exception:
                    errors.append("invalid connector configuration")
        elif k == "ModelProvider":
            p = target(s["destinationPolicy"], {"Policy"})
            if p and p["spec"]["type"] != "modelDestination":
                errors.append("wrong model destination policy")
            if "secret" in s:
                target(s["secret"], {"SecretReference"})
        elif k == "Model":
            target(s["provider"], {"ModelProvider"})
            if "embeddings" in s["capabilities"] and "embeddingDimensions" not in s:
                errors.append("embedding dimensions missing")
            if not s["mutable"] and "versionLabel" not in s:
                errors.append("immutable model requires version label")
        elif k == "Agent":
            target(s["adapter"], {"AgentAdapter"})
            target(s["application"], {"Application"})
            target(s["model"], {"Model"})
        elif k == "RetrievalCollection":
            src = target(s["source"], {"ObjectType"})
            model = target(s["embeddingModel"], {"Model"})
            if src and s["textProperty"] not in src["spec"]["properties"]:
                errors.append("unknown retrieval property")
            if model and "embeddings" not in model["spec"]["capabilities"]:
                errors.append("retrieval requires embedding model")
            if s["chunkOverlap"] >= s["chunkSize"]:
                errors.append("chunk overlap must be smaller than chunk")
        elif k == "EvaluationSuite":
            target(s["agent"], {"Agent"})
            target(s["fixtures"], {"Dataset"})
        elif k == "Automation":
            target(
                s["target"],
                {
                    "build": {"Transform"},
                    "sync": {"Connection"},
                    "invokeAgent": {"Agent"},
                    "evaluate": {"EvaluationSuite"},
                }[s["operation"]],
            )
            target(s["application"], {"Application"})
        elif k == "Grant":
            target(s["application"], {"Application"})
            target(s["target"], set(x["kind"] for x in items.values()))
        elif k == "SecretReference":
            for val in s["consumers"]:
                target(val, set(x["kind"] for x in items.values()))
        elif k == "Policy":
            if s["type"] == "modelDestination":
                for v in s["allowedMarkings"]:
                    target(v, {"Marking"})
            else:
                expr_context(
                    s["expression"],
                    (
                        {"principalAttribute"}
                        if s["type"] in {"propertyRead", "propertyWrite"}
                        else {"field", "principalAttribute"}
                    ),
                )
        elif k == "ObjectSet":
            target(s["objectType"], {"ObjectType"})

    def visit(node, stack, done):
        if node in stack:
            errors.append("transform cycle")
            return
        if node in done:
            return
        for ds in graph.get(node, []):
            if ds in producers:
                visit(producers[ds], stack | {node}, done)
        done.add(node)

    done = set()
    for key in graph:
        visit(key, set(), done)
    return errors


manifest = load("examples/maintenance.json")
mv.validate(manifest)
assert not semantic_checks(manifest), semantic_checks(manifest)
kinds = {r["kind"] for r in manifest["resources"]}
declared = {r["$ref"].split("/")[-1] for r in resources_schema["$defs"]["Resource"]["oneOf"]}
assert kinds == declared, "every declared resource kind needs an example"
negative = load("examples/invalid-resources.json")
for case in negative:
    assert not rv.is_valid(case["resource"]), "negative accepted: " + case["name"]


def mutation(kind, fn):
    x = deepcopy(manifest)
    fn(next(r for r in x["resources"] if r["kind"] == kind))
    return x


semantic_negative = [
    (
        "restricted view cannot be transform input",
        mutation(
            "Transform",
            lambda r: r["spec"].update({"inputs": ["maintenance/RestrictedView/RegionalOrders"]}),
        ),
    ),
    (
        "unresolved target",
        mutation(
            "RestrictedView", lambda r: r["spec"].update({"dataset": "maintenance/Dataset/missing"})
        ),
    ),
    (
        "wrong reference kind",
        mutation(
            "RestrictedView",
            lambda r: r["spec"].update({"dataset": "maintenance/ObjectType/WorkOrder"}),
        ),
    ),
    (
        "wrong mapped column",
        mutation(
            "ObjectType", lambda r: r["spec"]["source"]["mapping"].update({"status": "missing"})
        ),
    ),
    (
        "wrong object uid type",
        mutation(
            "ObjectType",
            lambda r: r["spec"]["properties"].update({"id": {"type": "string", "nullable": False}}),
        ),
    ),
    (
        "missing dataset primary key",
        mutation(
            "Dataset",
            lambda r: r["spec"].update({"primaryKey": ["missing"]}),
        ),
    ),
    (
        "bad decimal scale",
        mutation(
            "Dataset",
            lambda r: r["spec"]["columns"].update(
                {"amount": {"type": "decimal", "nullable": False, "precision": 2, "scale": 3}}
            ),
        ),
    ),
    (
        "transform self-cycle",
        mutation(
            "Transform",
            lambda r: r["spec"].update({"inputs": ["maintenance/Dataset/work_orders_clean"]}),
        ),
    ),
    (
        "invalid link property",
        mutation("LinkType", lambda r: r["spec"].update({"sourceProperty": "missing"})),
    ),
    (
        "missing target input",
        mutation("Action", lambda r: r["spec"].update({"targetParameter": "missing"})),
    ),
    (
        "bad chunk overlap",
        mutation("RetrievalCollection", lambda r: r["spec"].update({"chunkOverlap": 1001})),
    ),
    (
        "invalid connector config",
        mutation("Connection", lambda r: r["spec"].update({"configuration": {"unknown": "x"}})),
    ),
]
for name, doc in semantic_negative:
    mv.validate(doc)  # These must be structural passes, semantic failures.
    assert semantic_checks(doc), "semantic negative accepted: " + name

# Local Markdown links are packaging integrity checks, not web availability checks.
links = 0
for md in ROOT.rglob("*.md"):
    for value in re.findall(r"\]\(([^)]+)\)", md.read_text(encoding="utf-8")):
        if "://" in value or value.startswith("#"):
            continue
        file = value.split("#")[0]
        assert (md.parent / file).exists(), f"broken local link: {md}: {value}"
        links += 1

report = f"""# Specification validation report

Executed locally for the review package. This is not a platform runtime test report.

- JSON Schema Draft 2020-12 meta-validation: 3 schemas passed.
- Resource examples: {len(manifest['resources'])} passed; all {len(kinds)} declared kinds covered.
- Negative structural cases: {len(negative)} rejected as expected.
- Static cross-resource checks: example manifest passed.
- Negative semantic cases: {len(semantic_negative)} structurally valid cases rejected as expected.
- Local Markdown file links: {links} resolved.
- Validator: Python jsonschema {version('jsonschema')}, format checking enabled; schema references resolved offline.

Install `schemas/requirements.txt`, then run `python schemas/validate.py` from this package. The validator rewrites this report. The example uses placeholder artifact digests, endpoints, project IDs and UIDs; it is not a deployable platform.

Not tested: SQL policy enforcement, transaction races, worker isolation, model integrations, performance, installation or restore. Full expression typing, current grants, dependency packages, ownership and server-side execution checks are future compiler/runtime work. No Foundry reference conformance was executed.
"""
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--check", action="store_true", help="Validate without rewriting the report")
args = parser.parse_args()
if not args.check:
    (ROOT / "validation-report.md").write_text(report, encoding="utf-8")
print(report)
