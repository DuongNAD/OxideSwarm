"""
Milestone 1 Empirical Stress Test Suite (Challenger 1)

Checks:
1. All internal markdown links and anchor links across all tracked .md files in the repository.
2. Absence of CRLF (\\r\\n) in any tracked .sh shell scripts.
3. Genuine absence of all 6 legacy status files.
4. Presence, CFF 1.2.0 required fields, LF endings, and Cargo.toml workspace parity for CITATION.cff.
"""

import datetime
import os
import re
import subprocess
import unittest
import urllib.parse

try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11 fallback
    tomllib = None

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

FENCE_CHARS = ("```", "~~~")


def tracked_files_with_suffix(repo_root: str, suffix: str):
    """Returns tracked files ending with `suffix` via `git ls-files`, falling back to os.walk."""
    try:
        out = subprocess.check_output(
            ["git", "ls-files"],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        )
        paths = []
        for rel in out.splitlines():
            rel = rel.strip()
            if rel.endswith(suffix):
                full = os.path.join(repo_root, rel)
                if os.path.isfile(full):
                    paths.append(full)
        if paths:
            return paths
    except Exception:
        pass

    result = []
    for root, dirs, files in os.walk(repo_root):
        dirs[:] = [
            d
            for d in dirs
            if not d.startswith(".") and d not in ("target", "node_modules", ".agents")
        ]
        for file in files:
            if file.endswith(suffix):
                result.append(os.path.join(root, file))
    return result


def is_fence(line):
    s = line.strip()
    return any(s.startswith(fc) for fc in FENCE_CHARS)


def slugify_variants(text):
    """Returns GitHub-compatible markdown header slug variants (preserving per-space hyphens and collapsed hyphens)."""
    t = text.strip().lower()
    # Remove markdown link formatting e.g. [text](url) -> text
    t = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", t)
    # Remove backticks and inline code formatting
    t = t.replace("`", "")
    # Remove HTML tags if present e.g. <code>...</code>
    t = re.sub(r"<[^>]+>", "", t)
    # Remove characters that are not alphanumeric, whitespace, or hyphen
    t = re.sub(r"[^\w\s-]", "", t)
    # GitHub replaces each space with '-' after stripping punctuation (so ' / ' or ' — ' becomes '--')
    github_slug = t.replace(" ", "-")
    collapsed_slug = re.sub(r"\s+", "-", t)
    return [s for s in (github_slug, collapsed_slug) if s]


def extract_markdown_headers(content):
    """
    Extracts all header slugs from markdown content, respecting code fences
    and GitHub duplicate header suffixing (-1, -2, etc.).
    """
    slug_counts = {}
    valid_slugs = set()
    in_fence = False

    for line in content.splitlines():
        if is_fence(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue

        # Check for ATX headers (# Header)
        m = re.match(r"^(#{1,6})\s+(.*)$", line)
        if m:
            header_raw = m.group(2).strip()
            # Strip trailing hashes (e.g. '## Header ##')
            header_raw = re.sub(r"\s+#+$", "", header_raw)
            variants = slugify_variants(header_raw)
            if not variants:
                continue

            base_slug = variants[0]
            count = slug_counts.get(base_slug, 0)
            slug_counts[base_slug] = count + 1
            for v in variants:
                valid_slugs.add(v if count == 0 else f"{v}-{count}")
                valid_slugs.add(v)

    return valid_slugs

def parse_markdown_links(content):
    """
    Extracts all [text](target) links from markdown content,
    ignoring links inside fenced code blocks.
    Yields (line_number, link_text, link_target).
    """
    in_fence = False
    link_pattern = re.compile(r'!?\[([^\]]*)\]\(([^)]+)\)')

    for line_num, line in enumerate(content.splitlines(), start=1):
        if is_fence(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue

        for m in link_pattern.finditer(line):
            text = m.group(1)
            target = m.group(2).strip()
            yield (line_num, text, target)


REQUIRED_CFF_KEYS = (
    "cff-version",
    "message",
    "title",
    "abstract",
    "authors",
    "version",
    "date-released",
    "license",
    "repository-code",
    "keywords",
)


def _strip_yaml_comment(line: str) -> str:
    in_single = False
    in_double = False
    for idx, ch in enumerate(line):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            if idx == 0 or line[idx - 1].isspace():
                return line[:idx].rstrip()
    return line.rstrip()


def _unquote_yaml_scalar(value: str) -> str:
    v = value.strip()
    if len(v) >= 2 and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
        return v[1:-1]
    return v


def parse_top_level_cff(content: str) -> dict:
    """Pure-stdlib YAML-subset parser for top-level keys, scalars, and lists in CITATION.cff."""
    data = {}
    current_list_key = None

    for raw_line in content.splitlines():
        line = _strip_yaml_comment(raw_line)
        if not line.strip():
            continue

        # Top-level key (no leading whitespace)
        if not line[0].isspace():
            if ":" not in line:
                continue
            key, rest = line.split(":", 1)
            key = key.strip()
            val = rest.strip()
            current_list_key = None
            if not val:
                data[key] = []
                current_list_key = key
            elif val.startswith("[") and val.endswith("]"):
                inner = val[1:-1].strip()
                items = [_unquote_yaml_scalar(part) for part in inner.split(",") if part.strip()]
                data[key] = items
            else:
                data[key] = _unquote_yaml_scalar(val)
        elif current_list_key is not None:
            stripped = line.lstrip()
            if stripped.startswith("- "):
                raw_item = stripped[2:].strip()
                mapping_match = None
                if not raw_item.startswith(("'", '"')):
                    mapping_match = re.match(r"^([A-Za-z0-9_-]+)\s*:\s*(.*)$", raw_item)
                if mapping_match:
                    sub_key = mapping_match.group(1)
                    sub_val = _unquote_yaml_scalar(mapping_match.group(2))
                    data[current_list_key].append({sub_key: sub_val})
                else:
                    data[current_list_key].append(_unquote_yaml_scalar(raw_item))
            elif stripped == "-":
                data[current_list_key].append({})
            else:
                # Continuation mapping line under a list item (e.g., author subfields)
                if data[current_list_key]:
                    last = data[current_list_key][-1]
                    if isinstance(last, dict):
                        mapping_match = re.match(r"^([A-Za-z0-9_-]+)\s*:\s*(.*)$", stripped)
                        if mapping_match:
                            last[mapping_match.group(1)] = _unquote_yaml_scalar(mapping_match.group(2))
                    else:
                        data[current_list_key][-1] = f"{last} {stripped}".strip()

    return data


def load_workspace_package(cargo_toml_path: str) -> dict:
    """Reads [workspace.package] fields from Cargo.toml using stdlib tomllib or a <3.11 fallback."""
    if tomllib is not None:
        with open(cargo_toml_path, "rb") as f:
            parsed = tomllib.load(f)
        return dict(parsed.get("workspace", {}).get("package", {}))

    with open(cargo_toml_path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    pkg = {}
    in_section = False
    for raw_line in lines:
        stripped = raw_line.split("#", 1)[0].strip()
        if not stripped:
            continue
        if stripped.startswith("[") and stripped.endswith("]"):
            in_section = stripped == "[workspace.package]"
            continue
        if in_section and "=" in stripped:
            key, val = stripped.split("=", 1)
            pkg[key.strip()] = _unquote_yaml_scalar(val.strip())
    return pkg


class TestMilestone1EmpiricalStress(unittest.TestCase):

    def setUp(self):
        self.repo_root = REPO_ROOT

    def test_no_crlf_in_shell_scripts(self):
        """Verify that NO tracked shell script (.sh) in the repo contains CRLF or CR bytes."""
        sh_files = tracked_files_with_suffix(self.repo_root, ".sh")

        self.assertGreater(len(sh_files), 0, "Expected at least one .sh script in the repository.")
        
        crlf_violators = []
        for path in sh_files:
            rel_path = os.path.relpath(path, self.repo_root)
            with open(path, 'rb') as f:
                raw = f.read()
            if b'\r\n' in raw:
                crlf_violators.append((rel_path, "Contains CRLF (\\r\\n)"))
            elif b'\r' in raw:
                crlf_violators.append((rel_path, "Contains solitary CR (\\r)"))

        self.assertEqual(
            crlf_violators, [],
            f"Found shell scripts with carriage returns: {crlf_violators}"
        )
        print(f"\n[PASS] Verified {len(sh_files)} shell scripts are 100% free of CRLF/CR.")

    def test_all_6_legacy_files_genuinely_absent(self):
        """Verify that all 6 legacy/redundant files are absent from disk."""
        legacy_files = [
            "ORIGINAL_REQUEST.md",
            "TEST_READY.md",
            "TEST_INFRA.md",
            "upgrade_and_optimization_plan.md",
            os.path.join("docs", "WHAT_WAS_DONE.md"),
            os.path.join("scripts", "sync_network", "SUCCESS_CONFIRMED.md"),
        ]

        found_legacy = []
        for rel_path in legacy_files:
            full_path = os.path.join(self.repo_root, rel_path)
            if os.path.exists(full_path):
                found_legacy.append(rel_path)

        self.assertEqual(
            found_legacy, [],
            f"The following legacy files still exist on disk: {found_legacy}"
        )
        print(f"\n[PASS] Verified all {len(legacy_files)} legacy files are absent from disk.")

    def test_all_markdown_links_and_anchors(self):
        """
        Verify all internal markdown links and anchor links across all tracked .md files.
        Asserts 0 broken links.
        """
        md_files = tracked_files_with_suffix(self.repo_root, ".md")

        self.assertGreater(len(md_files), 0, "No markdown files found to verify.")

        # Precompute file headers and contents
        file_headers = {}
        file_contents = {}
        for path in md_files:
            norm_key = os.path.normcase(os.path.abspath(path))
            with open(path, 'r', encoding='utf-8') as f:
                c = f.read()
            file_contents[norm_key] = c
            file_headers[norm_key] = extract_markdown_headers(c)

        broken_links = []
        total_links = 0
        internal_links = 0

        for path in md_files:
            norm_key = os.path.normcase(os.path.abspath(path))
            content = file_contents[norm_key]
            rel_source = os.path.relpath(path, self.repo_root)
            dir_path = os.path.dirname(os.path.abspath(path))

            for line_num, link_text, link_target in parse_markdown_links(content):
                total_links += 1

                # Clean target (strip title attribute if present: e.g., 'target.md "title"')
                clean_target = link_target.split()[0] if link_target.split() else link_target
                # Remove enclosing angle brackets if present: e.g., '<#anchor>'
                if clean_target.startswith('<') and clean_target.endswith('>'):
                    clean_target = clean_target[1:-1]

                # Ignore external URLs
                if re.match(r'^[a-zA-Z][a-zA-Z0-9+-.]*:', clean_target):
                    continue

                internal_links += 1

                if clean_target.startswith('#'):
                    # Intra-document anchor link
                    anchor = clean_target[1:]
                    current_headers = file_headers[norm_key]
                    if anchor not in current_headers:
                        broken_links.append({
                            'source': rel_source,
                            'line': line_num,
                            'link_text': link_text,
                            'target': clean_target,
                            'reason': f"Anchor '#{anchor}' does not exist in '{rel_source}'"
                        })
                else:
                    # Target is a file path with optional #anchor
                    if '#' in clean_target:
                        target_file_part, target_anchor_part = clean_target.split('#', 1)
                    else:
                        target_file_part, target_anchor_part = clean_target, None

                    # Unquote URL encoding
                    target_file_part = urllib.parse.unquote(target_file_part)

                    # Resolve target path relative to source directory
                    target_abs_path = os.path.normpath(os.path.join(dir_path, target_file_part))

                    if not os.path.exists(target_abs_path):
                        broken_links.append({
                            'source': rel_source,
                            'line': line_num,
                            'link_text': link_text,
                            'target': clean_target,
                            'reason': f"Referenced file does not exist: '{target_file_part}' (resolved to '{target_abs_path}')"
                        })
                    elif target_anchor_part:
                        target_norm_key = os.path.normcase(os.path.abspath(target_abs_path))
                        if target_norm_key in file_headers:
                            target_headers = file_headers[target_norm_key]
                            if target_anchor_part not in target_headers:
                                broken_links.append({
                                    'source': rel_source,
                                    'line': line_num,
                                    'link_text': link_text,
                                    'target': clean_target,
                                    'reason': f"Anchor '#{target_anchor_part}' not found in '{target_file_part}'"
                                })

        error_message = "\n".join([
            f"  {b['source']}:{b['line']} -> [{b['link_text']}]({b['target']}) : {b['reason']}"
            for b in broken_links
        ])

        print(f"\n[INFO] Evaluated {total_links} total links across {len(md_files)} markdown files.")
        print(f"[INFO] Evaluated {internal_links} internal file and anchor links.")

        self.assertEqual(
            len(broken_links), 0,
            f"Found {len(broken_links)} broken internal markdown link(s):\n{error_message}"
        )
        print(f"[PASS] 0 broken internal links or anchors detected.")

    def test_citation_cff_matches_cargo_workspace(self):
        """Verify CITATION.cff exists with LF endings, required CFF 1.2.0 keys, and Cargo.toml parity."""
        cff_path = os.path.join(self.repo_root, "CITATION.cff")
        cargo_path = os.path.join(self.repo_root, "Cargo.toml")
        self.assertTrue(os.path.isfile(cff_path), "CITATION.cff must exist at repository root.")

        with open(cff_path, "rb") as f:
            raw = f.read()
        self.assertNotIn(b"\r\n", raw, "CITATION.cff must use LF endings, not CRLF.")
        self.assertNotIn(b"\r", raw, "CITATION.cff must use LF endings, not solitary CR.")

        content = raw.decode("utf-8")
        cff = parse_top_level_cff(content)

        for key in REQUIRED_CFF_KEYS:
            self.assertIn(key, cff, f"CITATION.cff missing required key '{key}'.")
            self.assertTrue(cff[key], f"CITATION.cff key '{key}' must be non-empty.")

        self.assertEqual(cff["cff-version"], "1.2.0")
        self.assertIsInstance(cff["date-released"], str)
        self.assertRegex(cff["date-released"], r"^\d{4}-\d{2}-\d{2}$")
        try:
            datetime.date.fromisoformat(cff["date-released"])
        except ValueError as exc:
            self.fail(f"CITATION.cff date-released is not a valid calendar date: {cff['date-released']!r} ({exc})")

        self.assertIsInstance(cff["repository-code"], str)
        self.assertTrue(
            cff["repository-code"].startswith("https://"),
            f"repository-code must start with https://, got {cff['repository-code']!r}",
        )
        self.assertIsInstance(cff["authors"], list)
        self.assertGreaterEqual(len(cff["authors"]), 1)
        for author in cff["authors"]:
            self.assertIsInstance(
                author,
                dict,
                f"CITATION.cff authors entries must be CFF 1.2.0 entity/person mappings, got {author!r}",
            )
            has_entity_name = isinstance(author.get("name"), str) and bool(author["name"].strip())
            has_person_name = (
                isinstance(author.get("family-names"), str) and bool(author["family-names"].strip())
            ) or (
                isinstance(author.get("given-names"), str) and bool(author["given-names"].strip())
            )
            self.assertTrue(
                has_entity_name or has_person_name,
                f"CITATION.cff author mapping must define non-empty 'name' or 'family-names'/'given-names', got {author!r}",
            )

        self.assertIsInstance(cff["keywords"], list)
        self.assertGreaterEqual(len(cff["keywords"]), 1)
        for kw in cff["keywords"]:
            self.assertIsInstance(kw, str)
            self.assertTrue(kw.strip(), "CITATION.cff keywords entries must be non-empty strings.")

        cargo_pkg = load_workspace_package(cargo_path)
        self.assertEqual(
            cff["version"],
            cargo_pkg.get("version"),
            "CITATION.cff version must match Cargo.toml [workspace.package].version.",
        )

        cargo_license = cargo_pkg.get("license", "")
        expected_licenses = {part.strip() for part in cargo_license.split(" OR ") if part.strip()}
        cff_licenses_raw = cff["license"]
        if len(expected_licenses) > 1:
            self.assertIsInstance(
                cff_licenses_raw,
                list,
                "CITATION.cff license must be a YAML list when Cargo.toml specifies multiple licenses.",
            )
        if isinstance(cff_licenses_raw, list):
            for item in cff_licenses_raw:
                self.assertIsInstance(item, str, "CITATION.cff license entries must be strings.")
                self.assertTrue(item.strip(), "CITATION.cff license entries must be non-empty.")
                self.assertNotIn(
                    " OR ",
                    item,
                    f"CITATION.cff license entry must be a single SPDX identifier, not an expression: {item!r}",
                )
            actual_licenses = set(cff_licenses_raw)
        elif isinstance(cff_licenses_raw, str):
            self.assertNotIn(
                " OR ",
                cff_licenses_raw,
                f"CITATION.cff license must be a single SPDX identifier, not an expression: {cff_licenses_raw!r}",
            )
            actual_licenses = {cff_licenses_raw}
        else:
            self.fail(
                f"CITATION.cff license must be a string or list of strings, got {type(cff_licenses_raw).__name__}"
            )
        self.assertEqual(
            actual_licenses,
            expected_licenses,
            "CITATION.cff license set must match Cargo.toml [workspace.package].license.",
        )
        print(f"\n[PASS] CITATION.cff valid (CFF 1.2.0, version {cff['version']}, licenses {sorted(actual_licenses)}).")

    def test_gitattributes_pins_cff_eol_lf(self):
        """Verify .gitattributes pins *.cff / CITATION.cff to text eol=lf for Windows checkouts."""
        gitattributes_path = os.path.join(self.repo_root, ".gitattributes")
        self.assertTrue(os.path.isfile(gitattributes_path), ".gitattributes must exist at repository root.")
        with open(gitattributes_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
        cff_rules = [
            ln for ln in lines
            if not ln.lstrip().startswith("#")
            and re.match(r"^(?:\*\.cff|CITATION\.cff)\s+.*\beol=lf\b", ln.strip())
        ]
        self.assertTrue(
            cff_rules,
            ".gitattributes must pin *.cff (or CITATION.cff) with text eol=lf.",
        )
        out = subprocess.check_output(
            ["git", "check-attr", "text", "eol", "--", "CITATION.cff"],
            cwd=self.repo_root,
            text=True,
        )
        self.assertIn("text: set", out)
        self.assertIn("eol: lf", out)


if __name__ == '__main__':
    unittest.main()
