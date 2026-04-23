# Class presentation — 10-minute lecture

`class_presentation.md` is a [Marp](https://marp.app/) deck. Three ways to use it:

### 1. Render to PDF / PPTX locally
```bash
npx @marp-team/marp-cli class_presentation.md -o slides.pdf
npx @marp-team/marp-cli class_presentation.md -o slides.pptx
```

### 2. Live preview in VS Code
Install the **Marp for VS Code** extension, open the file, click the preview pane.

### 3. Present straight from the markdown
Marp CLI has a `--server` mode:
```bash
npx @marp-team/marp-cli --server .
```
then open `localhost:8080/class_presentation.md`.

---

## Timing (10:00 total — short deck)

| # | Slide                          | Duration |
|---|--------------------------------|----------|
| 1 | Title                          | 0:20     |
| 2 | Hook                           | 0:40     |
| 3 | Thesis one-liner               | 0:25     |
| 4 | Why BNPL is invisible          | 0:50     |
| 5 | 4-gate architecture (+SVG)     | 1:10     |
| 6 | The trade                      | 0:40     |
| 7 | Why the engineering is the edge (+SVG) | 0:50 |
| 8 | What BSI looks like (fig1)     | 0:30     |
| 9 | Granger falsification (fig3)   | 0:55     |
|10 | Visual falsification (fig4)    | 0:35     |
|11 | The REGZ save (fig9)           | 0:45     |
|12 | Demo                           | 1:00     |
|13 | The paper                      | 0:25     |
|14 | What's next                    | 0:25     |
|15 | Q&A                            | remainder|

For the **deep deck** (`class_presentation_deep.md`, 18 min) see the
timing comment at the top of that file — 28 slides including a BSI
worked-example walkthrough, dynamic-threshold disclosure, and a
cumulative-equity-curve SVG.

Speaker notes are in HTML comments inside each slide. Marp hides them
from the rendered deck but surfaces them in presenter mode.

## Audience assumption
Technical but not finance-trained. Every finance term is unpacked in
one plain-English sentence the first time it appears. No jargon
victory laps.
