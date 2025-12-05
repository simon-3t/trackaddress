import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Solana transaction JSON to a flat CSV for accountability.",
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to the JSON file produced by solscan_fetch.py.",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Destination CSV file to create.",
    )
    return parser.parse_args()


def load_transactions(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def format_timestamp(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def iter_rows(address: str, tx: dict):
    base = {
        "address": address,
        "signature": tx.get("signature", ""),
        "type": tx.get("type", ""),
        "source": tx.get("source", ""),
        "slot": tx.get("slot", ""),
        "timestamp": format_timestamp(tx["timestamp"]) if "timestamp" in tx else "",
        "fee": tx.get("fee", ""),
        "fee_payer": tx.get("feePayer", ""),
        "description": tx.get("description", ""),
    }

    native_transfers = tx.get("nativeTransfers") or []
    token_transfers = tx.get("tokenTransfers") or []

    if native_transfers:
        for transfer in native_transfers:
            yield {
                **base,
                "transfer_type": "native",
                "mint": "SOL",
                "amount": transfer.get("amount", ""),
                "from_account": transfer.get("fromUserAccount", ""),
                "to_account": transfer.get("toUserAccount", ""),
            }

    if token_transfers:
        for transfer in token_transfers:
            yield {
                **base,
                "transfer_type": "token",
                "mint": transfer.get("mint", ""),
                "amount": transfer.get("tokenAmount", ""),
                "from_account": transfer.get("fromUserAccount", ""),
                "to_account": transfer.get("toUserAccount", ""),
            }

    if not native_transfers and not token_transfers:
        yield {
            **base,
            "transfer_type": "none",
            "mint": "",
            "amount": "",
            "from_account": "",
            "to_account": "",
        }


def write_csv(rows, output: Path) -> None:
    fieldnames = [
        "address",
        "signature",
        "type",
        "source",
        "slot",
        "timestamp",
        "fee",
        "fee_payer",
        "transfer_type",
        "mint",
        "amount",
        "from_account",
        "to_account",
        "description",
    ]

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    data = load_transactions(args.input)

    rows = []
    for address, transactions in data.items():
        for tx in transactions:
            rows.extend(iter_rows(address, tx))

    write_csv(rows, args.output)


if __name__ == "__main__":
    main()
