#!/usr/bin/env python3
"""
infer_lineage_from_specs.py — TraderX bespoke lineage

Reads this repo's spec-kit corpus (specs/**/system/messaging-subject-map.md,
specs/**/system/architecture.model.json, specs/**/data-model.md) and asks
Claude to reconstruct the current data-flow graph -- which table is written
downstream of which other table, mediated by which Java service -- forced
into a fixed JSON shape via output_config.format (structured outputs), with
every edge required to cite the exact spec evidence that justifies it.

This is necessary because TraderX's Trades/Positions tables are written by
Java microservices (order-matcher, trade-processor) via NATS, not SQL --
there is no view or query for DataHub's native ingestion source to parse, so
lineage here can only ever be asserted, never inferred from SQL.

Everything after the LLM call is NOT a stand-in: every dataset/table name
the model returns is validated against what's actually ingested in DataHub
before anything is emitted, exactly as a hand-written lineage script would.

Usage:
    python tools/infer_lineage_from_specs.py --dry-run
    python tools/infer_lineage_from_specs.py --server http://localhost:9080 --token <tok>

Environment variables (alternative to CLI args):
    DATAHUB_SERVER     DataHub GMS URL (default: http://localhost:8080)
    DATAHUB_TOKEN      DataHub access token
    ANTHROPIC_API_KEY  Anthropic API key (required unless --dry-run)
"""

import argparse
import glob
import json
import os
import sys

import anthropic
import requests

PLATFORM = "postgres"
ENV = "PROD"
DATABASE = "traderx"

CANDIDATE_TABLES = ["accounts", "accountusers", "orderbook", "trades", "positions"]

INFERRED_BY = "claude-opus-5 (structured-output inference over specs/** corpus)"

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _dataset_urn(table: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{PLATFORM},{DATABASE}.public.{table},{ENV})"


FLOW_URN = "urn:li:dataFlow:(traderx,trade-lifecycle,PROD)"

LINEAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "edges": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "downstream": {"type": "string", "enum": CANDIDATE_TABLES},
                    "upstreams": {"type": "array", "items": {"type": "string", "enum": CANDIDATE_TABLES}},
                    "evidence": {"type": "string", "description": "Exact spec file path(s) and node/edge names that justify this edge."},
                },
                "required": ["downstream", "upstreams", "evidence"],
                "additionalProperties": False,
            },
        },
        "datajob": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Short kebab-case service name, e.g. trade-processor."},
                "description": {"type": "string"},
                "inputs": {"type": "array", "items": {"type": "string", "enum": CANDIDATE_TABLES}},
                "outputs": {"type": "array", "items": {"type": "string", "enum": CANDIDATE_TABLES}},
            },
            "required": ["name", "description", "inputs", "outputs"],
            "additionalProperties": False,
        },
        "origination_only": {
            "type": "array",
            "items": {"type": "string", "enum": CANDIDATE_TABLES},
            "description": "Tables with no upstream by design (master/reference data written directly by a service, not derived from another table).",
        },
    },
    "required": ["edges", "datajob", "origination_only"],
    "additionalProperties": False,
}


# ---------------------------------------------------------------------------
# Step 1: gather the spec corpus this repo actually ships
# ---------------------------------------------------------------------------

def collect_spec_corpus(repo_root: str) -> str:
    patterns = [
        "specs/*/system/messaging-subject-map.md",
        "specs/*/system/architecture.model.json",
        "specs/*/data-model.md",
    ]
    parts = []
    for pattern in patterns:
        for path in sorted(glob.glob(os.path.join(repo_root, pattern))):
            rel = os.path.relpath(path, repo_root)
            with open(path) as fh:
                parts.append(f"=== {rel} ===\n{fh.read()}")
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Step 2: ask Claude to reconstruct the lineage graph, evidence-cited
# ---------------------------------------------------------------------------

def call_llm_for_lineage(repo_root: str) -> dict:
    corpus = collect_spec_corpus(repo_root)

    prompt = f"""You are reconstructing the current data-flow graph of TraderX (a FINOS
reference trading application) for a data catalog, from its spec-kit corpus.

TraderX's live Postgres database (database "traderx", schema "public") has exactly
these tables: {", ".join(CANDIDATE_TABLES)}. These are written directly by Java and
TypeScript microservices -- not by SQL views or queries -- so lineage between them
can only be reconstructed by reading the architecture docs below, never by parsing SQL.

Below is the full corpus of this repo's spec-kit artifacts across all of its
incremental feature states: each state's messaging-subject-map.md (NATS subject
producer/consumer map), architecture.model.json (service topology graph), and
data-model.md (entity-to-table deltas).

Read all of it and determine:
1. Which table is downstream of which other table, mediated by a service (an
   "edge") -- e.g. does a filled order become a trade? Does a trade produce a
   position update? For every edge, cite the exact spec file path(s) and the
   specific node/edge names or subject names that justify it. Do not assert an
   edge you cannot point to real evidence for in the corpus below.
2. The one service most responsible for writing the transactional tables
   (trades/positions) -- its name, a one-sentence description, and its input
   and output tables.
3. Which tables have no upstream by design (origination points -- master or
   reference data written directly by a service, not derived from another
   table) -- list them in origination_only.

Only ever reference the {len(CANDIDATE_TABLES)} table names listed above.

=== SPEC CORPUS ===
{corpus}
"""

    client = anthropic.Anthropic()
    response = client.messages.create(
        model="claude-opus-5",
        max_tokens=4096,
        output_config={"format": {"type": "json_schema", "schema": LINEAGE_SCHEMA}},
        messages=[{"role": "user", "content": prompt}],
    )

    if response.stop_reason == "refusal":
        raise RuntimeError(f"Claude declined the request: {response.stop_details}")

    text = next(block.text for block in response.content if block.type == "text")
    return json.loads(text)


def build_inferred_graph(repo_root: str) -> dict:
    raw = call_llm_for_lineage(repo_root)

    edges = [
        {
            "downstream": _dataset_urn(e["downstream"]),
            "upstreams": [_dataset_urn(u) for u in e["upstreams"]],
            "evidence": e["evidence"],
        }
        for e in raw["edges"]
    ]

    dj = raw["datajob"]
    job_urn = f"urn:li:dataJob:({FLOW_URN},{dj['name']})"

    return {
        "edges": edges,
        "datajob": {
            "urn": job_urn,
            "flow_urn": FLOW_URN,
            "name": dj["name"],
            "description": dj["description"],
            "inputs": [_dataset_urn(t) for t in dj["inputs"]],
            "outputs": [_dataset_urn(t) for t in dj["outputs"]],
        },
        "origination_only": [_dataset_urn(t) for t in raw["origination_only"]],
    }


# ---------------------------------------------------------------------------
# HTTP helpers (same ingestProposal convention as assign_domains.py / setup_domains.py)
# ---------------------------------------------------------------------------

def ingest(server: str, token: str, entity_urn: str, entity_type: str,
           aspect_name: str, aspect: dict) -> bool:
    url = f"{server.rstrip('/')}/aspects?action=ingestProposal"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    payload = {
        "proposal": {
            "entityType": entity_type,
            "entityUrn": entity_urn,
            "aspectName": aspect_name,
            "changeType": "UPSERT",
            "aspect": {"contentType": "application/json", "value": json.dumps(aspect)},
        }
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    return True


def dataset_exists(server: str, token: str, dataset_urn: str) -> bool:
    """Validation guardrail: don't assert lineage onto a dataset that was never ingested.

    GMS returns 200 with a synthesized DatasetKey-only entity even for URNs that were
    never actually ingested, so "any aspects at all" is not a valid existence check --
    look for an aspect that only appears once a real source has written to this URN.
    """
    url = f"{server.rstrip('/')}/entities/{requests.utils.quote(dataset_urn, safe='')}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, timeout=15)
    if resp.status_code != 200:
        return False
    body = resp.json()
    aspects = body.get("value", {}).get("com.linkedin.metadata.snapshot.DatasetSnapshot", {}).get("aspects", [])
    aspect_types = {next(iter(a.keys())) for a in aspects if a}
    return "com.linkedin.common.Status" in aspect_types


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(server: str, token: str, dry_run: bool) -> None:
    print(f"DataHub server : {server}")
    print(f"Dry run        : {dry_run}")
    print()

    print("=== Asking Claude to reconstruct lineage from specs/** ===")
    graph = build_inferred_graph(REPO_ROOT)
    print(f"  {len(graph['edges'])} edge(s), 1 datajob ({graph['datajob']['name']}), "
          f"{len(graph['origination_only'])} origination-only table(s)")
    print()

    print("=== Validating inferred edges against live DataHub ===")
    valid_edges = []
    for edge in graph["edges"]:
        downstream = edge["downstream"]
        missing_upstreams = []
        for up in edge["upstreams"]:
            exists = True if dry_run else dataset_exists(server, token, up)
            if not exists:
                missing_upstreams.append(up)

        downstream_exists = True if dry_run else dataset_exists(server, token, downstream)

        if missing_upstreams or not downstream_exists:
            print(f"  SKIP  {downstream}")
            if not downstream_exists:
                print(f"        downstream dataset not found in DataHub -- was it ingested yet?")
            for up in missing_upstreams:
                print(f"        upstream not found in DataHub, dropping edge: {up}")
                print(f"        (evidence was: {edge['evidence']})")
            still_valid_upstreams = [u for u in edge["upstreams"] if u not in missing_upstreams]
            if still_valid_upstreams and downstream_exists:
                edge = {**edge, "upstreams": still_valid_upstreams}
                valid_edges.append(edge)
                print(f"        proceeding with remaining upstream(s): {still_valid_upstreams}")
            continue

        print(f"  OK    {downstream}  <-  {edge['upstreams']}")
        valid_edges.append(edge)

    print()
    print("=== Emitting lineage ===")
    ok, err = 0, 0
    for edge in valid_edges:
        aspect = {
            "upstreams": [
                {
                    "dataset": up,
                    "type": "TRANSFORMED",
                    "properties": {
                        "inferredBy": INFERRED_BY,
                        "evidence": edge["evidence"],
                    },
                }
                for up in edge["upstreams"]
            ]
        }
        print(f"  UPSERT upstreamLineage  {edge['downstream']} ...", end=" ", flush=True)
        if dry_run:
            print("DRY")
            ok += 1
            continue
        try:
            ingest(server, token, edge["downstream"], "dataset", "upstreamLineage", aspect)
            print("OK")
            ok += 1
        except requests.RequestException as exc:
            print(f"ERROR: {exc}")
            err += 1

    print()
    print("=== Validating DataJob inputs/outputs against live DataHub ===")
    dj = graph["datajob"]
    valid_inputs = []
    for ds in dj["inputs"]:
        exists = True if dry_run else dataset_exists(server, token, ds)
        print(f"  {'OK' if exists else 'SKIP'}    input   {ds}")
        if exists:
            valid_inputs.append(ds)
    valid_outputs = []
    for ds in dj["outputs"]:
        exists = True if dry_run else dataset_exists(server, token, ds)
        print(f"  {'OK' if exists else 'SKIP'}    output  {ds}")
        if exists:
            valid_outputs.append(ds)

    print()
    print("=== Emitting DataFlow / DataJob nodes ===")
    flow_aspect = {"name": "TraderX Trade Lifecycle", "description": "Live microservice pipeline turning orders into trades and positions."}
    job_info_aspect = {
        "name": dj["name"],
        "type": {"string": "COMMAND"},
        "flowUrn": dj["flow_urn"],
        "description": dj["description"],
        "customProperties": {"inferredBy": INFERRED_BY},
    }
    job_io_aspect = {"inputDatasets": valid_inputs, "outputDatasets": valid_outputs}

    for entity_urn, entity_type, aspect_name, aspect in [
        (dj["flow_urn"], "dataFlow", "dataFlowInfo", flow_aspect),
        (dj["urn"], "dataJob", "dataJobInfo", job_info_aspect),
        (dj["urn"], "dataJob", "dataJobInputOutput", job_io_aspect),
    ]:
        print(f"  UPSERT {aspect_name}  {entity_urn} ...", end=" ", flush=True)
        if dry_run:
            print("DRY")
            ok += 1
            continue
        try:
            ingest(server, token, entity_urn, entity_type, aspect_name, aspect)
            print("OK")
            ok += 1
        except requests.RequestException as exc:
            print(f"ERROR: {exc}")
            err += 1

    print()
    print("=== Summary ===")
    print(f"  {'Would emit' if dry_run else 'Emitted'} : {ok}")
    print(f"  Errors               : {err}")
    if graph["origination_only"]:
        print(f"  Origination-only datasets (no upstream by design): {graph['origination_only']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", default=os.environ.get("DATAHUB_SERVER", "http://localhost:8080"))
    parser.add_argument("--token", default=os.environ.get("DATAHUB_TOKEN"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.dry_run and not args.token:
        print("ERROR: --token or DATAHUB_TOKEN required (or use --dry-run)", file=sys.stderr)
        sys.exit(1)

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: ANTHROPIC_API_KEY required -- this script calls Claude even in --dry-run "
              "(dry-run only skips writing to DataHub, not the inference step)", file=sys.stderr)
        sys.exit(1)

    run(server=args.server, token=args.token, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
