# Marle-G — Claude Artifacts design pipeline

A repeatable way to design new pod pages visually in **claude.ai (Artifacts)** and drop them
into the live pod with almost no rework.

## The pipeline
1. Open a **new chat at claude.ai**.
2. **Attach two files** from `indian-stock-market-tracker/web/`:
   - `_template.html`  (the page scaffold — nav, clock, guide, refresh, component vocabulary)
   - `marle_g.css`     (the design system — tokens + components)
3. **Paste the prompt below**, filling the three blanks.
4. Iterate on the look in the Artifact preview (it uses sample data — it CAN'T reach your localhost API).
5. Paste the resulting HTML back to me (Claude Code). I wire it to the live `/api/*` endpoint,
   add it to `marle_g_nav.js`, and verify it in the browser.

> Why Artifacts and not Google Stitch: Artifacts outputs real runnable code that already matches
> your design system (because you give it the template), so porting is copy-paste, not a rebuild.
> The only thing it can't do — reach your private localhost API — no external design tool can.

---

## The prompt (copy, fill the three blanks)

> You are designing one new page for an existing dark-themed Indian-stock-market dashboard called **Marle-G**.
>
> I've attached two files that define the design system: **`marle_g.css`** (all color tokens and component
> classes) and **`_template.html`** (the page scaffold). **Match them exactly** — same dark theme, the
> saffron `#ff9933` / green accents, the `--mono` font for data, and the existing component classes
> (`.nav`, `.wrap`, `.head`, `.clock`, `.cards`/`.mc`, `.card`, `.sh`, `table.h`, `.sug`, `.badge`,
> `.seg`, `.btn`). Do not invent a new visual language.
>
> Build a **single self-contained `.html` file** (link `marle_g.css`, keep the `<script src="marle_g_nav.js">`
> tag and the `.nav` block and the dual IST·US-CT `#clock` exactly as in the template, and keep
> `window.podRefresh = load`).
>
> **The page:** _______________________________________________
> *(e.g. "a sector-rotation heatmap: 11 NSE sectors as tiles colored by 1-day and 5-day return, click a
> tile to list its constituents")*
>
> **The data** it will eventually load from (use realistic SAMPLE data inline for now — you can't reach the
> real API): _______________________________________________
> *(paste the JSON shape, e.g. `{ "asof": "...", "sectors": [{ "name": "Auto", "r1d": 1.2, "r5d": -0.4, "names": [...] }] }`)*
>
> **Sections I want:** _______________________________________________
> *(e.g. "a KPI row, the heatmap grid, and a constituents table that fills when a tile is clicked")*
>
> Use the component builders from the template (`statCard`, `dataTable`) where they fit, add new small
> ones if needed, and keep all interactivity in vanilla JS (no frameworks, no external libs except
> lightweight-charts if a candlestick is required). Output the complete HTML file.

---

## After Artifacts builds it
Paste it back here. I'll:
- swap the sample data for `fetch('/api/<your-endpoint>')`,
- add the matching endpoint to `marleg_server.py` if it doesn't exist,
- register the page in `marle_g_nav.js`,
- add a guide story for it in `marle_g_guide.js`,
- and verify it live in your browser.
