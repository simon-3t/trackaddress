import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Dict, Iterable, List


MIN_NATIVE_TRANSFER_LAMPORTS = 5_000  # 0.000005 SOL – filters rent/technical dust
getcontext().prec = 28


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Solana transaction JSON to a simplified accounting CSV.",
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


def lamports_to_sol(lamports: int) -> Decimal:
    return Decimal(lamports) / Decimal(1_000_000_000)


def format_decimal(amount: Decimal, precision: int) -> str:
    formatted = f"{amount:.{precision}f}"
    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    return formatted or "0"


def normalize_token_amount(raw_amount) -> Decimal:
    try:
        return Decimal(str(raw_amount))
    except (TypeError, ValueError, ArithmeticError):
        return Decimal(0)


def summarize_changes(address: str, tx: dict) -> Dict[str, Dict[str, Decimal]]:
    """Aggregate incoming and outgoing amounts for each asset.

    Returns a mapping of asset -> {"in": float, "out": float}.
    """

    changes: Dict[str, Dict[str, Decimal]] = defaultdict(
        lambda: {"in": Decimal(0), "out": Decimal(0)}
    )

    native_transfers = tx.get("nativeTransfers") or []
    token_transfers = tx.get("tokenTransfers") or []

    for transfer in native_transfers:
        amount = int(transfer.get("amount") or 0)
        if amount < MIN_NATIVE_TRANSFER_LAMPORTS:
            # Filter out rent/technical dust that isn't meaningful for accounting.
            continue

        from_user = transfer.get("fromUserAccount")
        to_user = transfer.get("toUserAccount")

        if from_user == to_user == address:
            continue
        if from_user == address:
            changes["SOL"]["out"] += Decimal(amount)
        if to_user == address:
            changes["SOL"]["in"] += Decimal(amount)

    for transfer in token_transfers:
        amount = normalize_token_amount(transfer.get("tokenAmount"))
        mint = transfer.get("mint") or "UNKNOWN"
        if amount <= 0:
            continue

        from_user = transfer.get("fromUserAccount")
        to_user = transfer.get("toUserAccount")

        if from_user == to_user == address:
            continue
        if from_user == address:
            changes[mint]["out"] += amount
        if to_user == address:
            changes[mint]["in"] += amount

    return changes


def iter_rows(address: str, tx: dict) -> Iterable[dict]:
    date = format_timestamp(tx["timestamp"]) if "timestamp" in tx else ""
    signature = tx.get("signature", "")
    fee_sol = lamports_to_sol(int(tx.get("fee") or 0))

    changes = summarize_changes(address, tx)
    if not changes:
        return []

    rows: List[dict] = []
    for asset, values in changes.items():
        amount_in = values["in"]
        amount_out = values["out"]

        # Skip rows with no effective movement for the wallet.
        if amount_in == 0 and amount_out == 0:
            continue

        if asset == "SOL":
            amount_in = lamports_to_sol(int(amount_in))
            amount_out = lamports_to_sol(int(amount_out))

        rows.append(
            {
                "Date": date,
                "Transaction Hash": signature,
                "Asset": asset,
                "Amount_IN": format_decimal(amount_in, 9),
                "Amount_OUT": format_decimal(amount_out, 9),
                "Fee (SOL)": format_decimal(fee_sol, 9),
            }
        )

    return rows


def write_csv(rows: Iterable[dict], output: Path) -> None:
    fieldnames = [
        "Date",
        "Transaction Hash",
        "Asset",
        "Amount_IN",
        "Amount_OUT",
        "Fee (SOL)",
    ]

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    data = load_transactions(args.input)

    rows: List[dict] = []
    for address, transactions in data.items():
        for tx in transactions:
            rows.extend(iter_rows(address, tx))

    write_csv(rows, args.output)


if __name__ == "__main__":
    main()
