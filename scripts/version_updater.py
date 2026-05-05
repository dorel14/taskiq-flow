#!/usr/bin/env python3
"""
Taskiq-Flow Version Updater

Synchronizes version numbers across pyproject.toml, uv.lock, and documentation.
Supports automatic version bumping.

Usage:
    python scripts/version_updater.py --check      # Check version consistency
    python scripts/version_updater.py --bump <type>  # Bump version (major/minor/patch)
    python scripts/version_updater.py --set <version>  # Set explicit version
"""

import re
import sys
import argparse
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).parent.parent

def read_pyproject_version() -> str:
    """Extract version from pyproject.toml."""
    pyproject = PROJECT_ROOT / "pyproject.toml"
    content = pyproject.read_text(encoding="utf-8")
    match = re.search(r'version\s*=\s*"([^"]+)"', content)
    if not match:
        raise ValueError("Version not found in pyproject.toml")
    return match.group(1)

def write_pyproject_version(new_version: str) -> None:
    """Update version in pyproject.toml."""
    pyproject = PROJECT_ROOT / "pyproject.toml"
    content = pyproject.read_text(encoding="utf-8")
    new_content = re.sub(
        r'version\s*=\s*"[^"]+"',
        f'version = "{new_version}"',
        content
    )
    pyproject.write_text(new_content, encoding="utf-8")
    print(f"✅ Updated pyproject.toml to version {new_version}")

def read_uvlock_version() -> Optional[str]:
    """Extract version from uv.lock if present."""
    uvlock = PROJECT_ROOT / "uv.lock"
    if not uvlock.exists():
        return None
    content = uvlock.read_text(encoding="utf-8")
    # Find the package block for taskiq-flow
    match = re.search(
        r'\[\[package\]\]\s*name\s*=\s*"taskiq-flow".*?version\s*=\s*"([^"]+)"',
        content,
        re.DOTALL
    )
    return match.group(1) if match else None

def write_uvlock_version(new_version: str) -> None:
    """Update version in uv.lock."""
    uvlock = PROJECT_ROOT / "uv.lock"
    if not uvlock.exists():
        print("⚠️  uv.lock not found, skipping")
        return
    content = uvlock.read_text(encoding="utf-8")
    # Replace only the taskiq-flow version
    new_content = re.sub(
        r'(name\s*=\s*"taskiq-flow".*?version\s*=\s*")([^"]+)(")',
        f'\\g<1>{new_version}\\g<3>',
        content,
        flags=re.DOTALL
    )
    if new_content == content:
        print("⚠️  Could not find taskiq-flow version in uv.lock, manual update needed")
    else:
        uvlock.write_text(new_content, encoding="utf-8")
        print(f"✅ Updated uv.lock to version {new_version}")

def update_docs_version(old_version: str, new_version: str) -> None:
    """Update version strings in all documentation files."""
    docs_dir = PROJECT_ROOT / "docs"
    if not docs_dir.exists():
        print("⚠️  docs/ directory not found, skipping")
        return

    updated_files = []
    for md_file in docs_dir.rglob("*.md"):
        if md_file.name == "_config.yml":
            continue
        content = md_file.read_text(encoding="utf-8")
        if old_version in content:
            new_content = content.replace(old_version, new_version)
            md_file.write_text(new_content, encoding="utf-8")
            updated_files.append(md_file)

    if updated_files:
        print(f"✅ Updated {len(updated_files)} documentation files to version {new_version}")
    else:
        print("ℹ️  No documentation files needed version update")

def update_config_footer(new_version: str) -> None:
    """Update version in docs/_config.yml footer."""
    config_path = PROJECT_ROOT / "docs" / "_config.yml"
    if not config_path.exists():
        print("⚠️  docs/_config.yml not found, skipping footer update")
        return
    content = config_path.read_text(encoding="utf-8")
    # Update footer_content line
    new_content = re.sub(
        r'(footer_content:\s*"Documentation version:\s*)\d+\.\d+\.\d+(\s*\|)',
        f'\\g<1>{new_version}\\g<2>',
        content
    )
    if new_content != content:
        config_path.write_text(new_content, encoding="utf-8")
        print(f"✅ Updated _config.yml footer to version {new_version}")

def bump_version(version: str, bump_type: str) -> str:
    """Bump version according to semver: major/minor/patch."""
    parts = list(map(int, version.split('.')))
    if len(parts) != 3:
        raise ValueError(f"Invalid version format: {version}")
    
    major, minor, patch = parts
    
    if bump_type == "major":
        major += 1
        minor = 0
        patch = 0
    elif bump_type == "minor":
        minor += 1
        patch = 0
    elif bump_type == "patch":
        patch += 1
    else:
        raise ValueError(f"Invalid bump type: {bump_type}. Use major/minor/patch")
    
    return f"{major}.{minor}.{patch}"

def check_consistency() -> bool:
    """Check that all versions are synchronized."""
    pyproject_version = read_pyproject_version()
    uvlock_version = read_uvlock_version()
    
    print(f"📦 pyproject.toml version: {pyproject_version}")
    if uvlock_version:
        print(f"📦 uv.lock version: {uvlock_version}")
    else:
        print("📦 uv.lock: not found or no version")
    
    consistent = True
    if uvlock_version and uvlock_version != pyproject_version:
        print("❌ Version mismatch between pyproject.toml and uv.lock!")
        consistent = False
    else:
        print("✅ Versions are consistent")
    
    return consistent

def main():
    parser = argparse.ArgumentParser(description="Update Taskiq-Flow version across all files")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true", help="Check version consistency")
    group.add_argument("--bump", choices=["major", "minor", "patch"], help="Bump version part")
    group.add_argument("--set", metavar="VERSION", help="Set explicit version (X.Y.Z)")
    
    args = parser.parse_args()
    
    try:
        if args.check:
            ok = check_consistency()
            sys.exit(0 if ok else 1)
        
        current_version = read_pyproject_version()
        print(f"📌 Current version: {current_version}")
        
        if args.bump:
            new_version = bump_version(current_version, args.bump)
        elif args.set:
            if not re.match(r'^\d+\.\d+\.\d+$', args.set):
                print("❌ Version must be in X.Y.Z format")
                sys.exit(1)
            new_version = args.set
        
        if new_version == current_version:
            print("ℹ️  Version unchanged")
            sys.exit(0)
        
        print(f"🔼 Bumping version: {current_version} → {new_version}")
        
        # Update all files
        write_pyproject_version(new_version)
        write_uvlock_version(new_version)
        update_docs_version(current_version, new_version)
        update_config_footer(new_version)
        
        print(f"\n✅ Version successfully updated to {new_version}")
        print("\n💡 Next steps:")
        print(f"   git add -A")
        print(f'   git commit -m "bump: version {new_version}"')
        print(f"   git tag -a v{new_version} -m \"Release v{new_version}\"")
        print(f"   git push origin develop --follow-tags")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
