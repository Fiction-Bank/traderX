#!/usr/bin/env python3
"""
infer_lineage_from_specs.py — TraderX bespoke lineage (prototype)

PROTOTYPE NOTE: this stands in for step 2 of the real pipeline ("call an LLM
with the spec corpus, get back a JSON graph"). That step is hardcoded below
as EDGES / DATAJOBS instead of an actual API call, so this can run without
wiring an LLM API key. Everything downstream of that — validating the graph
against what's actually ingested in DataHub, and emitting it — is real, not
a stand-in, and is exactly what the CI-wired version would also do.

Each assertion below cites the spec file(s) and edge(s) that justify it, so
the reasoning is auditable rather than a black box:

  ORDERBOOK -> TRADES
    specs/009-order-management-matcher/system/architecture.model.json:
      order_matcher -> nats  ("Publishes fills and status")
      nats -> trade_processor  ("Delivers matcher-generated fills")
    specs/006-messaging-nats-replacement/system/architecture.model.json:
      tradeProcessor -> database  ("Persist trade/position state")
    i.e. a matched order becomes a fill event, consumed and persisted as a
    trade by trade-processor. Nowhere is this expressed in SQL — order-matcher
    and trade-processor are Java services connected by a NATS subject.

  TRADES -> POSITIONS
    specs/006-messaging-nats-replacement/system/architecture.model.json:
      tradeProcessor -> database  ("Persist trade/position state")
    trade-processor persists both trade and position state from the same
    consumed event, so a trade produces the corresponding position update.

The real (non-prototype) version replaces build_inferred_graph() with an LLM
call reading specs/**/system/messaging-subject-map.md,
specs/**/system/architecture.model.json, and specs/**/data-model.md, forced
into the same {edges, datajobs} JSON shape via a schema — everything else in
this file (validation, emission) stays the same.

Usage:
    python tools/infer_lineage_from_specs.py --dry-run
    python tools/infer_lineage_from_specs.py --server http://localhost:9080 --token <tok>

Environment variables (alternative to CLI args):
    DATAHUB_SERVER   DataHub GMS URL (default: http://localhost:8080)
    DATAHUB_TOKEN    DataHub access token
"""

import argparse
import json
import os
import sys

import requests

PLATFORM = "postgres"
ENV = "PROD"
DATABASE = "traderx"

INFERRED_BY = "prototype-manual-pass (stand-in for LLM step)"


def _dataset_urn(table: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{PLATFORM},{DATABASE}.public.{table},{ENV})"


ACCOUNTS = _dataset_urn("accounts")
ACCOUNT_USERS = _dataset_urn("accountusers")
ORDERBOOK = _dataset_urn("orderbook")
TRADES = _dataset_urn("trades")
POSITIONS = _dataset_urn("positions")

FLOW_URN = "urn:li:dataFlow:(traderx,trade-lifecycle,PROD)"
JOB_URN = "urn:li:dataJob:(urn:li:dataFlow:(traderx,trade-lifecycle,PROD),trade-processor)"


# ---------------------------------------------------------------------------
# Step 2 stand-in: the inferred graph (see module docstring for evidence)
# ---------------------------------------------------------------------------

def build_inferred_graph() -> dict:
    return {
        "edges": [
            {
                "downstream": TRADES,
                "upstreams": [ORDERBOOK],
                "evidence": (
                    "specs/009-order-management-matcher/system/architecture.model.json "
                    "(order_matcher->nats->trade_processor); "
                    "specs/006-messaging-nats-replacement/system/architecture.model.json "
                    "(tradeProcessor->database)"
                ),
            },
            {
                "downstream": POSITIONS,
                "upstreams": [TRADES],
                "evidence": (
                    "specs/006-messaging-nats-replacement/system/architecture.model.json "
                    "(tradeProcessor->database: persists trade/position state from the same event)"
                ),
            },
        ],
        "datajob": {
            "urn": JOB_URN,
            "flow_urn": FLOW_URN,
            "name": "trade-processor",
            "description": (
                "Consumes matcher-generated fills (from order-matcher via NATS) and "
                "direct trade submissions (from trade-service), persists trades and "
                "positions. Java service, not a SQL job -- lineage here can only ever "
                "be asserted, not inferred from a query."
            ),
            "inputs": [ORDERBOOK],
            "outputs": [TRADES, POSITIONS],
        },
        # Origination points -- no upstream by design, listed for completeness.
        "origination_only": [ACCOUNTS, ACCOUNT_USERS],
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
    graph = build_inferred_graph()
    print(f"DataHub server : {server}")
    print(f"Dry run        : {dry_run}")
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

    run(server=args.server, token=args.token, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
