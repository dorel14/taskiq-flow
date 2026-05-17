import logging
import re
from pathlib import Path

# Configuration
DOCS_DIR = Path("docs")
EXAMPLES_DIR = Path("examples")
OUTPUT_INDEX = Path("llms.txt")
OUTPUT_FULL = Path("llms-full.txt")
BASE_URL = "https://dorel14.github.io/taskiq-flow"

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger()


def clean_jekyll_markdown(content: str) -> str:
    """Nettoie le contenu Markdown en supprimant les éléments spécifiques à Jekyll."""
    # 1. Supprimer le front-matter YAML (entre les deux ---)
    content = re.sub(r"^---\s*\n.*?\n---\s*\n", "", content, flags=re.DOTALL)
    # 2. Supprimer les tags Liquid Jekyll {% ... %}
    content = re.sub(r"\{%.*?%\}", "", content)
    # 3. Supprimer les attributs de classe kramdown {: .note }
    content = re.sub(r"\{:.*?\}", "", content)
    return content.strip()


def generate() -> None:
    """
    Génère les fichiers de documentation LLM.

    Génère les fichiers llms.txt et llms-full.txt à partir
    des fichiers Markdown et des exemples Python.
    - llms.txt : Un index simple avec des liens vers les documents.
    - llms-full.txt : Le contenu complet de tous les documents et exemples.
    """
    full_md = ["# Taskiq-Flow : Full Technical Documentation\n"]
    index_md = [
        "# Taskiq-Flow\n",
        "> Orchestration de tâches asynchrones pour taskiq (Sequential & Dataflow).\n",
        "## Core Documentation\n",
    ]

    # --- PARTIE 1 : DOCS MARKDOWN ---
    # On trie pour avoir une logique (index, puis guides, puis api)
    for md_file in sorted(DOCS_DIR.rglob("*.md")):
        if md_file.name == "index.md" and md_file.parent == DOCS_DIR:
            continue  # On gère l'accueil séparément si besoin

        with Path.open(md_file, "r", encoding="utf-8") as f:
            raw_content = f.read()
            clean_content = clean_jekyll_markdown(raw_content)

            # Extraction d'un titre propre
            title = md_file.stem.replace("-", " ").title()
            rel_path = md_file.relative_to(DOCS_DIR).with_suffix("")

            index_md.append(f"- [{title}]({BASE_URL}/{rel_path})")
            full_md.append(f"\n\n## DOCUMENT: {title}\n")
            full_md.append(clean_content)

    # --- PARTIE 2 : EXEMPLES PYTHON ---
    index_md.append("\n## Code Examples & Recipes\n")
    full_md.append("\n\n# Code Examples\n")

    if EXAMPLES_DIR.exists():
        for py_file in sorted(EXAMPLES_DIR.glob("*.py")):
            with Path.open(py_file, "r", encoding="utf-8") as f:
                code = f.read()
                full_md.append(f"\n### Example: {py_file.name}\n")
                full_md.append(f"```python\n{code}\n```")
                index_md.append(
                    f"- [Example: {py_file.name}](https://github.com/dorel14/taskiq-flow/blob/master/examples/{py_file.name})"
                )

    # --- ÉCRITURE ---
    OUTPUT_INDEX.write_text("\n".join(index_md))
    OUTPUT_FULL.write_text("\n".join(full_md))
    logger.info(f"Fichiers {OUTPUT_INDEX} et {OUTPUT_FULL} générés avec succès.")


if __name__ == "__main__":
    generate()
