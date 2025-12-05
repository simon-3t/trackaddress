import argparse
import csv
import json
import os
import time
from typing import Dict, List, Sequence

from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# Helius mainnet endpoint; include your API key in the query string.
DEFAULT_HELIUS_URL = "https://api-mainnet.helius-rpc.com"
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


def fetch_transactions(address: str, api_url: str, api_key: str, limit: int) -> Dict:
    query = urlencode({"limit": limit})
    endpoint = (
        f"{api_url.rstrip('/')}/v0/addresses/{address}/transactions/?api-key={api_key}&{query}"
    )
    request = Request(url=endpoint)

    with urlopen(request, timeout=30) as response:  # nosec: B310
        payload = json.load(response)

    if isinstance(payload, dict) and payload.get("success") is False:
        raise RuntimeError(f"Helius error for {address}: {payload}")
    return payload


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
        help="Number of transactions to request per address.",
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get("HELIUS_API_URL", DEFAULT_HELIUS_URL),
        help="Base Helius API URL to use.",
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
            payload = fetch_transactions(address, args.api_url, args.api_key, args.limit)
        except HTTPError as exc:
            print(f"HTTP error for {address}: {exc}")
            continue
        except Exception as exc:  # noqa: BLE001
            print(f"Error for {address}: {exc}")
            continue

        results[address] = payload
        time.sleep(args.delay)

    write_results(args.output, results)
    print(f"Wrote transactions for {len(results)} address(es) to {args.output}")


if __name__ == "__main__":
    main()
