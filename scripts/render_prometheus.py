"""Render prometheus.yml from prometheus.yml.tmpl using env vars from .env.

Prometheus does not expand environment variables in its config file by design;
this is the standard workaround. Run before `docker compose up -d prometheus`.
"""
import os
from pathlib import Path
from string import Template

from dotenv import load_dotenv

REPO = Path(__file__).resolve().parents[1]
load_dotenv(REPO / ".env")

tmpl = (REPO / "prometheus.yml.tmpl").read_text()
rendered = Template(tmpl).safe_substitute(os.environ)
(REPO / "prometheus.yml").write_text(rendered)
print(f"rendered {REPO / 'prometheus.yml'}")
