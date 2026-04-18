"""Convert MASTERPLAN.md to PDF using xhtml2pdf with DejaVu fonts (pre-registered)."""
import shutil
from pathlib import Path
import markdown
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from xhtml2pdf import pisa

SRC_FONTS = Path(r"C:/Users/siddh/Desktop/fin 515 midterm/fonts/dejavu-fonts-ttf-2.37/ttf")
PROJ = Path(r"C:/Users/siddh/Desktop/spring 2026/580/BNPL")
LOCAL_FONTS = PROJ / "fonts"
LOCAL_FONTS.mkdir(exist_ok=True)

needed = {
    "DejaVuSans.ttf": "DejaVu",
    "DejaVuSans-Bold.ttf": "DejaVu-Bold",
    "DejaVuSans-Oblique.ttf": "DejaVu-Italic",
    "DejaVuSans-BoldOblique.ttf": "DejaVu-BoldItalic",
    "DejaVuSansCondensed.ttf": "DejaVuSans",
    "DejaVuSansCondensed-Bold.ttf": "DejaVuSans-Bold",
    "DejaVuSansMono.ttf": "DejaVuMono",
    "DejaVuSansMono-Bold.ttf": "DejaVuMono-Bold",
}
for fname in needed:
    dst = LOCAL_FONTS / fname
    if not dst.exists():
        shutil.copy(SRC_FONTS / fname, dst)

for fname, psname in needed.items():
    pdfmetrics.registerFont(TTFont(psname, str(LOCAL_FONTS / fname)))

pdfmetrics.registerFontFamily(
    "DejaVu",
    normal="DejaVu", bold="DejaVu-Bold",
    italic="DejaVu-Italic", boldItalic="DejaVu-BoldItalic",
)
pdfmetrics.registerFontFamily(
    "DejaVuSans", normal="DejaVuSans", bold="DejaVuSans-Bold",
    italic="DejaVuSans", boldItalic="DejaVuSans-Bold",
)
pdfmetrics.registerFontFamily(
    "DejaVuMono", normal="DejaVuMono", bold="DejaVuMono-Bold",
    italic="DejaVuMono", boldItalic="DejaVuMono-Bold",
)

SRC = PROJ / "MASTERPLAN.md"
OUT = PROJ / "MASTERPLAN.pdf"
md_text = SRC.read_text(encoding="utf-8")
html_body = markdown.markdown(
    md_text, extensions=["tables", "fenced_code", "sane_lists"],
)

css = """
@page {
  size: letter;
  margin: 0.9in 0.95in 0.9in 0.95in;
  @frame footer { -pdf-frame-content: footerContent; bottom: 0.4in; left: 0.95in; right: 0.95in; height: 0.3in; }
}
body { font-family: 'DejaVu'; font-size: 10pt; line-height: 1.45; color: #1a1a1a; text-align: justify; }
h1 { font-family: 'DejaVuSans'; font-size: 20pt; color: #111; margin-top: 18pt; margin-bottom: 8pt; border-bottom: 1.2pt solid #333; padding-bottom: 3pt; }
h2 { font-family: 'DejaVuSans'; font-size: 15pt; color: #111; margin-top: 16pt; margin-bottom: 6pt; border-bottom: 0.5pt solid #888; padding-bottom: 2pt; }
h3 { font-family: 'DejaVuSans'; font-size: 12pt; color: #222; margin-top: 12pt; margin-bottom: 4pt; }
h4 { font-family: 'DejaVuSans'; font-size: 11pt; color: #333; margin-top: 10pt; margin-bottom: 3pt; }
p  { margin: 4pt 0 6pt 0; }
strong { font-weight: bold; }
em { font-style: italic; }
code { font-family: 'DejaVuMono'; font-size: 8.5pt; background: #f2f2f2; padding: 1pt 2pt; }
pre { font-family: 'DejaVuMono'; font-size: 8pt; background: #f5f5f5; border: 0.4pt solid #ccc; padding: 6pt; line-height: 1.25; -pdf-keep-in-frame-mode: shrink; }
pre code { background: transparent; padding: 0; }
ul, ol { margin: 4pt 0 6pt 18pt; }
li { margin-bottom: 2pt; }
table { border-collapse: collapse; width: 100%; margin: 6pt 0; font-size: 9pt; -pdf-keep-in-frame-mode: shrink; }
th { background: #e6e6e6; font-weight: bold; text-align: left; padding: 3pt 5pt; border: 0.4pt solid #888; }
td { padding: 3pt 5pt; border: 0.4pt solid #bbb; vertical-align: top; }
hr { border: 0; border-top: 0.4pt solid #888; margin: 10pt 0; }
a  { color: #1a4b8c; text-decoration: none; }
blockquote { margin: 6pt 14pt; padding-left: 8pt; border-left: 2pt solid #888; color: #444; }
.footer { font-family: 'DejaVuSans'; font-size: 8pt; color: #777; text-align: center; }
"""

html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>{css}</style></head>
<body>
{html_body}
<div id="footerContent" class="footer">BNPL Agentic Pod — Master Implementation Plan · <pdf:pagenumber/> / <pdf:pagecount/></div>
</body></html>"""

with open(OUT, "wb") as fh:
    result = pisa.CreatePDF(html, dest=fh, encoding="utf-8")
if result.err:
    raise SystemExit(f"pisa errors: {result.err}")
print(f"OK -> {OUT} ({OUT.stat().st_size} bytes)")
