"""Expand source references from the checkout, then typeset the handbook with Pandoc and Chromium."""

import ast
import base64
import mimetypes
import html
from pathlib import Path
import re
import shutil
import subprocess
from playwright.sync_api import sync_playwright

DOCS = Path(__file__).resolve().parent
PROJECT = DOCS.parent


def excerpt(match):
    path, _, function = match.group(1).partition("#")
    source = (PROJECT / path).read_text(encoding="utf-8")
    if function:
        node = next(
            n
            for n in ast.walk(ast.parse(source))
            if isinstance(n, ast.FunctionDef) and n.name == function
        )
        source = ast.get_source_segment(source, node)
    escaped = html.escape(source.rstrip())
    label = html.escape(path + (" · " + function if function else ""))
    return f'<p class="listing">{label}</p><pre><code>{escaped}</code></pre>'


def build():
    for file in DOCS.glob("*.dot"):
        subprocess.run(
            ["dot", "-Tsvg", str(file), "-o", str(DOCS / "images" / (file.stem + ".svg"))], check=True
        )
    md = re.sub(r"^\{\{source (.+?)\}\}$", excerpt, (DOCS / "Handbook.md").read_text(), flags=re.M)
    body = subprocess.run(
        ["pandoc", "-f", "markdown+raw_html", "-t", "html5"],
        input=md,
        text=True,
        capture_output=True,
        check=True,
    ).stdout
    css = (DOCS / "handbook.css").read_text()
    cover = '<section class="cover"><p>BIG DATA SYSTEMS &nbsp; / &nbsp; LAB HANDBOOK</p><h1>Real-Time Restaurant Kitchen Order Monitoring</h1><p>Architecture, implementation and live demonstration</p><hr><p>Python · Kafka · NiFi · Spark · SQL Server · Streamlit</p></section>'
    for picture in (DOCS / "images").iterdir():
        mime = mimetypes.guess_type(picture.name)[0]
        encoded = base64.b64encode(picture.read_bytes()).decode()
        body = body.replace('src="images/' + picture.name + '"', f'src="data:{mime};base64,{encoded}"')
    document = (
        f'<html><head><meta charset="utf-8"><style>{css}</style></head><body>{cover}{body}</body></html>'
    )
    browser = shutil.which("chromium") or shutil.which("google-chrome")
    if not browser:
        raise RuntimeError("Install Chromium or Google Chrome to rebuild the PDF.")
    with sync_playwright() as automation:
        chrome = automation.chromium.launch(
            executable_path=browser, args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = chrome.new_page()
        page.set_content(document, wait_until="load", timeout=30000)
        assert page.locator("img").evaluate_all(
            "images => images.every(i => i.complete && i.naturalWidth > 0)"
        )
        page.pdf(
            path=str(DOCS / "Restaurant_Kitchen_Monitoring_Documentation.pdf"),
            prefer_css_page_size=True,
            print_background=True,
            outline=True,
        )
        chrome.close()


if __name__ == "__main__":
    build()
