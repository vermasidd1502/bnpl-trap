"""
Marle-G — read-only Groww account snapshot.

Pulls holdings / positions / orders / margin and prints a clean summary.
STRICTLY read-only: this module never places, modifies, or cancels an order.

  python marleg_groww_account.py            # human-readable summary
  python marleg_groww_account.py --json     # raw payloads as JSON
"""
import sys, json
import groww_client as gc


def fetch():
    c = gc.GrowwClient()
    return {
        "auth_mode": c.auth_mode,
        "holdings": (c.holdings_data() or {}).get("holdings") or [],
        "positions": (c.positions_data() or {}).get("positions") or [],
        "orders": (c.orders_data() or {}).get("order_list") or [],
        "margin": c.margin_data() or {},
    }


def _g(d, *keys):
    return {k: d.get(k) for k in keys if k in d}


def main():
    data = fetch()
    if "--json" in sys.argv:
        print(json.dumps(data, indent=2, default=str))
        return
    print(f"auth mode: {data['auth_mode']}\n")

    print("== HOLDINGS ==")
    for x in data["holdings"]:
        print(f"  {x.get('trading_symbol',''):<16} qty {x.get('quantity')}  avg {x.get('average_price')}")
    if not data["holdings"]:
        print("  (none)")

    print("\n== F&O / INTRADAY POSITIONS ==")
    if data["positions"]:
        print("  field names:", sorted(data["positions"][0].keys()))
    for x in data["positions"]:
        print("  ", json.dumps(_g(x, "trading_symbol", "segment", "product",
                                  "credit_quantity", "debit_quantity",
                                  "net_carry_forward_quantity", "quantity",
                                  "credit_price", "debit_price"), default=str))
    if not data["positions"]:
        print("  (none)")

    print("\n== OPEN / RECENT ORDERS ==")
    if data["orders"]:
        print("  field names:", sorted(data["orders"][0].keys()))
    for x in data["orders"][:10]:
        print("  ", json.dumps(_g(x, "trading_symbol", "transaction_type", "quantity",
                                  "order_status", "order_type", "price", "trigger_price"), default=str))
    if not data["orders"]:
        print("  (none)")

    m = data["margin"]
    print("\n== MARGIN ==")
    print("  ", json.dumps(_g(m, "clear_cash", "net_margin_used", "collateral_available",
                              "collateral_used", "brokerage_and_charges"), default=str))


if __name__ == "__main__":
    main()
