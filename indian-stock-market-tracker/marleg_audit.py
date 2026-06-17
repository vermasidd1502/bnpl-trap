"""marleg_audit.py — QA sweep: every pod page loads (200), every /api endpoint responds, every cross-pod
link resolves to a real file. Run against the live server. Prints only problems + a summary."""
import glob
import os
import re
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
WEB = os.path.join(HERE, "web")
BASE = "http://127.0.0.1:8777"


def hit(path):
    try:
        with urllib.request.urlopen(BASE + path, timeout=30) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:
        return "ERR:" + str(e)[:22]


def main():
    pages = sorted(os.path.basename(p) for p in glob.glob(os.path.join(WEB, "*.html")))
    pageset = set(pages)

    print(f"=== PAGES ({len(pages)}) ===")
    badp = []
    for pg in pages:
        c = hit("/" + pg)
        if c != 200:
            badp.append((pg, c)); print(f"  [FAIL] {pg} -> {c}")
    print(f"  {len(pages) - len(badp)}/{len(pages)} pages return 200")

    src = open(os.path.join(HERE, "marleg_server.py"), encoding="utf-8").read()
    apis = sorted(set(r for r in re.findall(r'@app\.route\(\s*["\']([^"\']+)["\']', src) if r.startswith("/api")))
    print(f"\n=== API ENDPOINTS ({len(apis)}) ===")
    bada = []
    for r in apis:
        test = re.sub(r'<[^>]+>', 'RELIANCE', r)
        c = hit(test)
        if c != 200:
            bada.append((r, c)); print(f"  [{c}] {r}" + (f"  (tested {test})" if test != r else ""))
    print(f"  {len(apis) - len(bada)}/{len(apis)} endpoints return 200")

    linked = set()
    for fn in [os.path.join(WEB, "marle_g_nav.js")] + [os.path.join(WEB, p) for p in pages]:
        try:
            linked |= set(re.findall(r'(marle_g_[a-z0-9_]+\.html)', open(fn, encoding="utf-8").read()))
        except Exception:
            pass
    missing = sorted(l for l in linked if l not in pageset)
    print(f"\n=== CROSS-POD LINKS ({len(linked)} distinct) ===")
    for m in missing:
        print(f"  [MISSING] {m} — linked but no file in web/")
    if not missing:
        print("  all linked pages exist")

    print(f"\nSUMMARY: pages {len(pages)-len(badp)}/{len(pages)} · endpoints {len(apis)-len(bada)}/{len(apis)} · broken links {len(missing)}")
    if badp:
        print("  broken pages:", badp)
    if bada:
        print("  non-200 endpoints:", [(r, c) for r, c in bada])


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
