"""
Milestone 1 Empirical Stress Test Suite (Challenger 1)

Checks:
1. All internal markdown links and anchor links across all .md files in the repository.
2. Absence of CRLF (\\r\\n) in any .sh shell scripts.
3. Genuine absence of all 6 legacy status files.
4. Integrity of .agents/teamwork/ORIGINAL_REQUEST.md, when that local file exists (.agents/ is
   gitignored scratch output, so a clean checkout skips this check).
"""

import os
import re
import unittest
import urllib.parse

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
AGENTS_ORIGINAL_REQUEST = os.path.join(REPO_ROOT, ".agents", "teamwork", "ORIGINAL_REQUEST.md")

FENCE_CHARS = ("```", "~~~")

def is_fence(line):
    s = line.strip()
    return any(s.startswith(fc) for fc in FENCE_CHARS)

def slugify(text):
    # GitHub markdown slugification
    t = text.strip().lower()
    # Remove markdown link formatting e.g. [text](url) -> text
    t = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', t)
    # Remove backticks and inline code formatting
    t = t.replace('`', '')
    # Remove HTML tags if present e.g. <code>...</code>
    t = re.sub(r'<[^>]+>', '', t)
    # Remove characters that are not alphanumeric, whitespace, or hyphen
    t = re.sub(r'[^\w\s-]', '', t)
    # Convert whitespace to single hyphen
    t = re.sub(r'\s+', '-', t)
    return t

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
        m = re.match(r'^(#{1,6})\s+(.*)$', line)
        if m:
            header_raw = m.group(2).strip()
            # Strip trailing hashes (e.g. '## Header ##')
            header_raw = re.sub(r'\s+#+$', '', header_raw)
            base_slug = slugify(header_raw)
            if not base_slug:
                continue

            count = slug_counts.get(base_slug, 0)
            if count == 0:
                slug_counts[base_slug] = 1
                valid_slugs.add(base_slug)
            else:
                slug_counts[base_slug] = count + 1
                valid_slugs.add(f"{base_slug}-{count}")

            # Also add raw simplified slug for resilience
            simple_slug = re.sub(r'[^\w\s-]', '', header_raw.lower()).strip()
            simple_slug = re.sub(r'\s+', '-', simple_slug)
            if simple_slug:
                valid_slugs.add(simple_slug)

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


class TestMilestone1EmpiricalStress(unittest.TestCase):

    def setUp(self):
        self.repo_root = REPO_ROOT

    def test_no_crlf_in_shell_scripts(self):
        """Verify that NO shell script (.sh) in the repo contains CRLF or CR bytes."""
        sh_files = []
        for root, dirs, files in os.walk(self.repo_root):
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ('target', 'node_modules', '.agents')]
            for file in files:
                if file.endswith('.sh'):
                    sh_files.append(os.path.join(root, file))

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

    @unittest.skipUnless(
        os.path.exists(AGENTS_ORIGINAL_REQUEST),
        ".agents/ is gitignored local scratch output, absent from a clean checkout",
    )
    def test_original_request_intact_in_agents_dir(self):
        """Verify .agents/teamwork/ORIGINAL_REQUEST.md is intact and valid when it exists."""
        path = AGENTS_ORIGINAL_REQUEST

        size = os.path.getsize(path)
        self.assertGreater(size, 4000, f"ORIGINAL_REQUEST.md file is unexpectedly small ({size} bytes).")

        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        required_sections = [
            "# Original User Request",
            "## Initial Request",
            "## Requirements",
            "### R1. Documentation Audit",
            "### R2. Functional Gap Completion Across All Layers",
            "### R3. Security, Wire Protocol & Process Lifecycle Hardening",
            "### R4. Environment Isolation & Cross-Platform Verification",
            "## Verification Resources",
            "## Acceptance Criteria",
        ]
        missing_sections = [sec for sec in required_sections if sec not in content]
        self.assertEqual(
            missing_sections, [],
            f".agents/teamwork/ORIGINAL_REQUEST.md is missing expected sections: {missing_sections}"
        )
        print(f"\n[PASS] Verified .agents/teamwork/ORIGINAL_REQUEST.md is intact ({size} bytes).")

    def test_all_markdown_links_and_anchors(self):
        """
        Verify all internal markdown links and anchor links across all .md files.
        Asserts 0 broken links.
        """
        md_files = []
        for root, dirs, files in os.walk(self.repo_root):
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ('target', 'node_modules', '.agents')]
            for file in files:
                if file.endswith('.md'):
                    md_files.append(os.path.join(root, file))

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


if __name__ == '__main__':
    unittest.main()
