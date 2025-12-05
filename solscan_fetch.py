import argparse
import csv
import json
import os
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# Helius mainnet endpoint for JSON-RPC requests; include your API key in the query string.
DEFAULT_HELIUS_RPC_URL = "https://api-mainnet.helius-rpc.com"
# Helius REST endpoint for enhanced transaction lookups.
DEFAULT_HELIUS_REST_URL = "https://api.helius.xyz"
DEFAULT_API_KEY = "dd1e72eb-f7c4-4914-844d-a0e1b8c15a10"


def read_addresses(path: str) -> List[str]:
    addresses: List[str] = []
    with open(path, newline="") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            for cell in row:
                address = cell.strip()
                if address:
                    addresses.append(address)
    return addresses


def dedupe_preserve_order(items: Sequence[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def fetch_signatures(
    address: str,
    rpc_url: str,
    api_key: str,
    limit: int,
    before: Optional[str] = None,
) -> Tuple[List[str], Optional[str]]:
    query_params = {"api-key": api_key}
    if before:
        query_params["before"] = before

    endpoint = f"{rpc_url.rstrip('/')}?{urlencode(query_params)}"
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": "signatures",
            "method": "getSignaturesForAddress",
            "params": [address, {"limit": limit, "before": before}],
        }
    ).encode("utf-8")

    request = Request(url=endpoint, data=body, headers={"Content-Type": "application/json"})

    with urlopen(request, timeout=30) as response:  # nosec: B310
        payload = json.load(response)

    result = payload.get("result") if isinstance(payload, dict) else None
    if result is None or not isinstance(result, list):
        raise RuntimeError(f"Unexpected signatures payload for {address}: {payload}")

    signatures = [entry.get("signature") for entry in result if isinstance(entry, dict)]
    signatures = [sig for sig in signatures if sig]
    next_before = signatures[-1] if len(result) == limit and signatures else None
    return signatures, next_before


def fetch_transactions_for_signatures(
    rest_url: str, api_key: str, signatures: Iterable[str]
) -> List[Dict]:
    payload = json.dumps({"transactions": list(signatures)}).encode("utf-8")
    # Helius expects the endpoint without a trailing slash before the query string;
    # including it results in a 404. Keep the path tight to avoid spurious errors.
    endpoint = f"{rest_url.rstrip('/')}/v0/transactions?api-key={api_key}"
    request = Request(
        url=endpoint, data=payload, headers={"Content-Type": "application/json"}
    )

    with urlopen(request, timeout=30) as response:  # nosec: B310
        tx_payload = json.load(response)

    if isinstance(tx_payload, dict) and tx_payload.get("success") is False:
        raise RuntimeError(f"Helius transaction error: {tx_payload}")
    if not isinstance(tx_payload, list):
        raise RuntimeError(f"Unexpected transactions payload: {tx_payload}")
    return tx_payload


def fetch_transactions_with_retries(
    rest_url: str,
    api_key: str,
    signatures: List[str],
    delay: float,
    max_attempts: int = 3,
) -> List[Dict]:
    remaining = list(signatures)
    collected: List[Dict] = []

    for attempt in range(1, max_attempts + 1):
        if not remaining:
            break

        page = fetch_transactions_for_signatures(rest_url, api_key, remaining)
        collected.extend(page)

        returned_signatures = {
            tx.get("signature") for tx in page if isinstance(tx, dict) and tx.get("signature")
        }
        remaining = [sig for sig in remaining if sig not in returned_signatures]

        if remaining and attempt < max_attempts:
            time.sleep(delay)

    if remaining:
        print(
            f"Warning: {len(remaining)} transaction(s) were missing after retries: {', '.join(remaining)}"
        )

    return collected


def fetch_all_transactions(
    address: str,
    rpc_url: str,
    rest_url: str,
    api_key: str,
    limit: int,
    delay: float,
    max_transactions: Optional[int] = None,
) -> List[Dict]:
    all_transactions: List[Dict] = []
    before: Optional[str] = None

    while True:
        signatures, before = fetch_signatures(
            address, rpc_url, api_key, limit, before=before
        )

        if not signatures:
            break

        transactions = fetch_transactions_with_retries(
            rest_url, api_key, signatures, delay
        )
        all_transactions.extend(transactions)

        if max_transactions is not None and len(all_transactions) >= max_transactions:
            return all_transactions[:max_transactions]

        if len(signatures) < limit or before is None:
            break

        time.sleep(delay)

    return all_transactions


def write_results(output_path: str, results: Dict[str, Dict]) -> None:
    with open(output_path, "w", encoding="utf-8") as outfile:
        json.dump(results, outfile, indent=2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Solana transactions for addresses using the Helius API.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        default="solana.csv",
        help="Path to CSV file with Solana addresses.",
    )
    parser.add_argument(
        "--output",
        default="transactions.json",
        help="Where to write the fetched transactions as JSON.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Page size for each API request (Helius max is typically 100).",
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get("HELIUS_API_URL", DEFAULT_HELIUS_RPC_URL),
        help="Helius RPC URL used for getSignaturesForAddress.",
    )
    parser.add_argument(
        "--rest-api-url",
        default=os.environ.get("HELIUS_REST_API_URL", DEFAULT_HELIUS_REST_URL),
        help="Helius REST URL used for transaction lookups.",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("HELIUS_API_KEY", DEFAULT_API_KEY),
        help="Helius API key.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay between API requests in seconds to avoid rate limits.",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="Maximum number of transactions to fetch per address (default: fetch all).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    addresses = dedupe_preserve_order(read_addresses(args.input))
    if not addresses:
        raise SystemExit("No addresses found in input file.")

    results: Dict[str, Dict] = {}
    for index, address in enumerate(addresses, start=1):
        print(f"[{index}/{len(addresses)}] Fetching transactions for {address}...")
        try:
            payload = fetch_all_transactions(
                address,
                args.api_url,
                args.rest_api_url,
                args.api_key,
                args.limit,
                args.delay,
                max_transactions=args.max,
            )
        except HTTPError as exc:
            print(f"HTTP error for {address}: {exc}")
            continue
        except Exception as exc:  # noqa: BLE001
            print(f"Error for {address}: {exc}")
            continue

        results[address] = payload

    write_results(args.output, results)
    print(f"Wrote transactions for {len(results)} address(es) to {args.output}")


if __name__ == "__main__":
    main()
