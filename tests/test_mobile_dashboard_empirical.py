#!/usr/bin/env python3
"""
Empirical Mobile Dashboard Stress & Verification Test Suite
Tests across mobile viewports:
- 360x800 (Android Standard)
- 375x667 (iPhone SE / 8)
- 390x844 (iPhone 13 / 14 / 15)
- 412x915 (Google Pixel 7 / 8)
- 320x568 (Ultra-Narrow Boundary)

Validates:
1. Zero empty voids & layout overflow on load
2. Terminal buffer & scroll position retention across tab switches
3. MASTER node rendering in #deviceList (position, green dot, role badge)
4. Fault injection: HTTP 500 handling (latency pill red 'Err' or 'Offline')
5. Quote escaping in escapeHtml()
"""

import http.server
import json
import os
import sys
import threading
import time
import shutil
from playwright.sync_api import sync_playwright

DASHBOARD_PATH = os.path.abspath("crates/master/src/dashboard.html")

MOCK_STATUS = {
    "master": {
        "host": "OxideSwarm-Master-Node",
        "role": "MASTER",
        "description": "P2P Cluster Coordinator"
    },
    "workers": [
        {
            "id": "worker-mac-orch-001",
            "name": "MacBook-Pro-Orchestrator",
            "status": "Connected",
            "role": "ORCH",
            "role_description": "Coordination & Build",
            "active_tasks": 1,
            "cpu_cores": 10,
            "ram_mb": 32768,
            "cpu_usage_pct": 22.4,
            "ram_available_mb": 21500,
            "has_gpu": False,
            "gpu_device_name": None,
            "battery_pct": 92,
            "is_charging": True,
            "thermal_throttled": False,
            "last_heartbeat_secs_ago": 1
        },
        {
            "id": "worker-win-gpu-002",
            "name": "Case-PC-RTX-4090",
            "status": "Busy",
            "role": "GPU",
            "role_description": "GPU Acceleration",
            "active_tasks": 2,
            "cpu_cores": 24,
            "ram_mb": 65536,
            "cpu_usage_pct": 88.5,
            "ram_available_mb": 14000,
            "has_gpu": True,
            "gpu_device_name": "NVIDIA GeForce RTX 4090",
            "battery_pct": None,
            "is_charging": None,
            "thermal_throttled": False,
            "last_heartbeat_secs_ago": 2
        }
    ],
    "tasks": {
        "total": 3,
        "queued": 0,
        "running": 3,
        "completed": 0,
        "failed": 0,
        "active_list": []
    }
}

server_status_code = 200

class DashboardHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        global server_status_code
        if self.path == "/" or self.path.startswith("/?"):
            try:
                with open(DASHBOARD_PATH, "rb") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(str(e).encode())
        elif self.path == "/api/status":
            if server_status_code == 200:
                body = json.dumps(MOCK_STATUS).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(server_status_code)
                self.send_header("Content-Type", "text/plain")
                body = b"Internal Server Error (Simulated 500)"
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

def run_suite():
    global server_status_code
    port = 19123
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), DashboardHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    base_url = f"http://127.0.0.1:{port}"
    results = {}

    viewports = [
        ("360x800", 360, 800),
        ("375x667", 375, 667),
        ("390x844", 390, 844),
        ("412x915", 412, 915),
        ("320x568", 320, 568),
    ]

    with sync_playwright() as p:
        chrome_bin = (
            shutil.which("chrome")
            or shutil.which("google-chrome")
            or shutil.which("chromium")
            or shutil.which("msedge")
        )
        launch_kwargs = {"headless": True}
        if chrome_bin:
            launch_kwargs["executable_path"] = chrome_bin
        browser = p.chromium.launch(**launch_kwargs)

        print("\n=================================================================")
        print("  1. VIEWPORT & EMPTY VOID STRESS TESTS")
        print("=================================================================")
        void_results = []
        for name, width, height in viewports:
            page = browser.new_page(viewport={"width": width, "height": height})
            page.goto(base_url)
            page.wait_for_timeout(350)

            metrics = page.evaluate("""() => {
                const doc = document.documentElement;
                const body = document.body;
                const layout = document.getElementById('mainLayout');
                const topBar = document.querySelector('.top-bar');
                const tabBar = document.querySelector('.tab-bar');
                const tabChat = document.getElementById('tabChat');
                const chatFeed = document.getElementById('chatFeed');
                const cmdBar = document.querySelector('.cmd-bar');

                return {
                    viewportH: window.innerHeight,
                    viewportW: window.innerWidth,
                    docScrollH: doc.scrollHeight,
                    docScrollW: doc.scrollWidth,
                    bodyScrollH: body.scrollHeight,
                    bodyScrollW: body.scrollWidth,
                    hasHorizontalOverflow: doc.scrollWidth > window.innerWidth || body.scrollWidth > window.innerWidth,
                    layoutH: layout ? layout.offsetHeight : 0,
                    topBarH: topBar ? topBar.offsetHeight : 0,
                    tabBarH: tabBar ? tabBar.offsetHeight : 0,
                    tabChatH: tabChat ? tabChat.offsetHeight : 0,
                    chatFeedH: chatFeed ? chatFeed.offsetHeight : 0,
                    cmdBarH: cmdBar ? cmdBar.offsetHeight : 0,
                };
            }""")

            # Void check: does layout fill viewport without excess blank void or horizontal overflow?
            # A void occurs if layout collapses to < 80% of viewport or if elements fail to render.
            has_void = (
                metrics["hasHorizontalOverflow"]
                or metrics["layoutH"] < (height * 0.8)
                or metrics["chatFeedH"] < 50
            )

            void_results.append({
                "viewport": name,
                "metrics": metrics,
                "has_void": has_void
            })

            print(f"  Viewport {name:7s} -> Doc: {metrics['docScrollW']}x{metrics['docScrollH']}, "
                  f"FeedH: {metrics['chatFeedH']}px, Overflow: {metrics['hasHorizontalOverflow']}, Void: {has_void}")
            page.close()

        results["viewport_void_tests"] = void_results

        print("\n=================================================================")
        print("  2. TERMINAL BUFFER & SCROLL POSITION RETENTION")
        print("=================================================================")
        page = browser.new_page(viewport={"width": 390, "height": 844})
        page.goto(base_url)
        page.wait_for_timeout(350)

        # Inject 40 log lines into chatFeed to create substantial scrollable history
        injection_status = page.evaluate("""() => {
            const feed = document.getElementById('chatFeed');
            for (let i = 1; i <= 40; i++) {
                const line = document.createElement('div');
                line.className = 'prompt-line';
                line.textContent = `LOG_LINE_${i}: Test terminal buffer preservation entry ${i}`;
                feed.appendChild(line);
            }
            feed.scrollTop = 420;
            return {
                initialLines: feed.children.length,
                setScrollTop: feed.scrollTop,
                scrollHeight: feed.scrollHeight
            };
        }""")
        print(f"  Injected lines: {injection_status['initialLines']}, Target ScrollTop: {injection_status['setScrollTop']}")

        # Switch to 'devices' tab
        page.locator("#btnTabDevices").click()
        page.wait_for_timeout(200)

        devices_active = page.evaluate("""() => {
            const tabDevices = document.getElementById('tabDevices');
            const tabChat = document.getElementById('tabChat');
            return {
                devicesActive: tabDevices.classList.contains('active'),
                chatActive: tabChat.classList.contains('active'),
                chatDisplay: window.getComputedStyle(tabChat).display
            };
        }""")
        print(f"  Switched to Devices tab: devicesActive={devices_active['devicesActive']}, chatDisplay={devices_active['chatDisplay']}")

        # Switch back to 'chat' tab
        page.locator("#btnTabChat").click()
        page.wait_for_timeout(200)

        retention_status = page.evaluate("""() => {
            const feed = document.getElementById('chatFeed');
            const lines = feed.children.length;
            const currentScrollTop = feed.scrollTop;
            const hasLogLine40 = feed.innerText.includes('LOG_LINE_40');
            return {
                finalLines: lines,
                finalScrollTop: currentScrollTop,
                hasLogLine40: hasLogLine40
            };
        }""")

        buffer_preserved = retention_status["finalLines"] == injection_status["initialLines"] and retention_status["hasLogLine40"]
        scroll_preserved = retention_status["finalScrollTop"] == injection_status["setScrollTop"]

        print(f"  Buffer Preserved   : {buffer_preserved} (Lines: {retention_status['finalLines']})")
        print(f"  ScrollTop Retained : {scroll_preserved} (Initial: {injection_status['setScrollTop']}px, Final: {retention_status['finalScrollTop']}px)")

        results["tab_retention"] = {
            "buffer_preserved": buffer_preserved,
            "scroll_preserved": scroll_preserved,
            "initial_scroll": injection_status["setScrollTop"],
            "final_scroll": retention_status["finalScrollTop"]
        }
        page.close()

        print("\n=================================================================")
        print("  3. MASTER NODE RENDERING IN #deviceList")
        print("=================================================================")
        page = browser.new_page(viewport={"width": 390, "height": 844})
        page.goto(base_url)
        page.wait_for_timeout(400)

        # Switch to devices tab and inspect #deviceList
        page.locator("#btnTabDevices").click()
        page.wait_for_timeout(300)

        device_cards = page.evaluate("""() => {
            const list = document.getElementById('deviceList');
            if (!list) return { error: '#deviceList not found' };
            const cards = list.querySelectorAll('.device-card');
            const items = [];
            cards.forEach((c, idx) => {
                const roleBadge = c.querySelector('.role-badge');
                const dot = c.querySelector('.status-dot');
                const name = c.querySelector('.node-name');
                items.append ? null : null;
                items.push({
                    index: idx,
                    role: roleBadge ? roleBadge.textContent.trim() : null,
                    roleClass: roleBadge ? roleBadge.className : null,
                    dotClass: dot ? dot.className : null,
                    name: name ? name.textContent.trim() : null,
                    text: c.innerText
                });
            });
            return {
                totalCards: cards.length,
                items: items,
                rawHtml: list.innerHTML
            };
        }""")

        total_cards = device_cards.get("totalCards", 0)
        items = device_cards.get("items", [])
        print(f"  Total cards rendered in #deviceList: {total_cards}")
        for item in items:
            print(f"    Card [{item['index']}]: Role='{item['role']}', Dot='{item['dotClass']}', Name='{item['name']}'")

        first_is_master = False
        master_found = False
        master_green_dot = False
        master_role_badge = False

        if items:
            first_card = items[0]
            if first_card["role"] == "MASTER" or "tag-master" in (first_card["roleClass"] or ""):
                first_is_master = True

        for item in items:
            if item["role"] == "MASTER" or (item["name"] and "Master" in item["name"]):
                master_found = True
                if item["dotClass"] and "dot-online" in item["dotClass"]:
                    master_green_dot = True
                if item["role"] == "MASTER":
                    master_role_badge = True

        print(f"  MASTER Node Found in #deviceList : {master_found}")
        print(f"  Rendered at the Top (Index 0)    : {first_is_master}")
        print(f"  Has Green Dot (.dot-online)       : {master_green_dot}")
        print(f"  Has Role Badge ('MASTER')         : {master_role_badge}")

        results["master_node_check"] = {
            "total_cards": total_cards,
            "master_found": master_found,
            "first_is_master": first_is_master,
            "master_green_dot": master_green_dot,
            "master_role_badge": master_role_badge,
            "items": items
        }
        page.close()

        print("\n=================================================================")
        print("  4. FAULT INJECTION: HTTP 500 ERROR HANDLING")
        print("=================================================================")
        # Set server to return 500
        server_status_code = 500

        page = browser.new_page(viewport={"width": 390, "height": 844})
        page.goto(base_url)
        page.wait_for_timeout(300)

        # Trigger sync via button
        page.locator("#syncBtn").click()
        page.wait_for_timeout(400)

        fault_eval = page.evaluate("""() => {
            const pill = document.getElementById('latencyPill');
            const summary = document.getElementById('clusterSummary');
            return {
                pillText: pill ? pill.textContent.trim() : null,
                pillColor: pill ? window.getComputedStyle(pill).color : null,
                summaryText: summary ? summary.textContent.trim() : null,
                summaryColor: summary ? window.getComputedStyle(summary).color : null,
            };
        }""")

        # Check if pill displays 'Err' or 'Offline' with red color
        is_pill_err = fault_eval["pillText"] in ("Err", "Offline")
        # In CSS: var(--dot-red) = #ef4444 -> rgb(239, 68, 68)
        is_pill_red = fault_eval["pillColor"] in ("rgb(239, 68, 68)", "rgb(220, 38, 38)") or "239, 68, 68" in (fault_eval["pillColor"] or "")

        print(f"  Pill Text  : '{fault_eval['pillText']}' (Expected 'Err' or 'Offline') -> {is_pill_err}")
        print(f"  Pill Color : '{fault_eval['pillColor']}' (Expected red rgb(239, 68, 68)) -> {is_pill_red}")
        print(f"  Summary    : '{fault_eval['summaryText']}'")

        results["fault_injection"] = {
            "is_pill_err": is_pill_err,
            "is_pill_red": is_pill_red,
            "details": fault_eval
        }
        page.close()
        server_status_code = 200

        print("\n=================================================================")
        print("  5. HTML ESCAPING VERIFICATION IN escapeHtml()")
        print("=================================================================")
        page = browser.new_page(viewport={"width": 390, "height": 844})
        page.goto(base_url)
        page.wait_for_timeout(300)

        test_payloads = [
            ("double_quote", 'hello"world', 'hello&quot;world'),
            ("single_quote", "hello'world", "hello&#039;world"),
            ("ampersand", "foo&bar", "foo&amp;bar"),
            ("angle_brackets", "<script>alert(1)</script>", "&lt;script&gt;alert(1)&lt;/script&gt;"),
            ("attribute_injection", 'test" onmouseover="alert(1)"', 'test&quot; onmouseover=&quot;alert(1)&quot;'),
            ("single_attr_breakout", "test' onclick='evil()'", "test&#039; onclick=&#039;evil()&#039;"),
        ]

        escape_results = []
        all_escaped_cleanly = True
        for label, raw_input, expected in test_payloads:
            actual = page.evaluate(f"() => escapeHtml({json.dumps(raw_input)})")
            passed = (actual == expected)
            if not passed:
                all_escaped_cleanly = False
            escape_results.append({
                "label": label,
                "input": raw_input,
                "expected": expected,
                "actual": actual,
                "passed": passed
            })
            print(f"  [{label}] Input: {raw_input!r:32s} -> Actual: {actual!r:38s} [PASS: {passed}]")

        results["escape_html"] = {
            "all_passed": all_escaped_cleanly,
            "tests": escape_results
        }
        page.close()

        browser.close()

    server.shutdown()
    print("\n=================================================================")
    print("  EMPIRICAL TEST RUN SUMMARY")
    print("=================================================================")
    all_voids_ok = all(not v["has_void"] for v in results["viewport_void_tests"])
    tab_retention_ok = results["tab_retention"]["buffer_preserved"] and results["tab_retention"]["scroll_preserved"]
    master_ok = results["master_node_check"]["first_is_master"] and results["master_node_check"]["master_green_dot"]
    fault_ok = results["fault_injection"]["is_pill_err"] and results["fault_injection"]["is_pill_red"]
    escape_ok = results["escape_html"]["all_passed"]

    print(f"  1. Viewport Voids & Overflow (5 Viewports)  : {'PASS' if all_voids_ok else 'FAIL'}")
    print(f"  2. Tab Switching Buffer & Scroll Retention : {'PASS' if tab_retention_ok else 'FAIL'}")
    print(f"  3. MASTER Node Rendered Top of #deviceList : {'PASS' if master_ok else 'FAIL'}")
    print(f"  4. Fault Injection (500 -> Red Err Pill)   : {'PASS' if fault_ok else 'FAIL'}")
    print(f"  5. escapeHtml() Quotes & Special Chars     : {'PASS' if escape_ok else 'FAIL'}")

    overall_pass = all_voids_ok and tab_retention_ok and master_ok and fault_ok and escape_ok
    print(f"\n  OVERALL MOBILE DASHBOARD VERDICT: {'CONFIRMED (PASS)' if overall_pass else 'FAILED'}")
    print("=================================================================\n")

    return results

if __name__ == "__main__":
    res = run_suite()
    # Save results as JSON for audit
    with open("tests/empirical_results.json", "w") as f:
        json.dump(res, f, indent=2)
