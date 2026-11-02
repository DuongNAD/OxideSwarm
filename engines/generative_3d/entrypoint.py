#!/usr/bin/env python3
"""
OxideSwarm Local Generative 3D Engine Entrypoint (TripoSR + Fallback)
File: engines/generative_3d/entrypoint.py
"""

import argparse
import os
import sys

def parse_args():
    parser = argparse.ArgumentParser(description="OxideSwarm 3D Mesh Generator")
    parser.add_argument("--input", type=str, default=None, help="Input concept image path")
    parser.add_argument("--prompt", type=str, default=None, help="Input text prompt")
    parser.add_argument("--output", type=str, required=True, help="Destination .glb path")
    parser.add_argument("--device", type=str, default="cuda:0", help="Execution device (cuda:0, cpu)")
    parser.add_argument("--quality", type=str, default="standard", help="Quality tier (fast, standard, high)")
    parser.add_argument("--export-texture", action="store_true", default=True, help="Bake UV textures")
    parser.add_argument("--no-texture", action="store_false", dest="export_texture")
    parser.add_argument("--offline", action="store_true", default=True, help="Force offline mode")
    return parser.parse_args()

def main():
    args = parse_args()

    if args.offline:
        os.environ["HF_HUB_OFFLINE"] = "1"
        os.environ["TRANSFORMERS_OFFLINE"] = "1"

    # OxideSwarm does not bundle a pretrained TripoSR neural network weights checkpoint.
    # Under Decision D3 and Finding H1, always fail honestly until a genuine model is integrated.
    sys.stderr.write("TripoSR model not available: local AI 3D model checkpoint is not installed\n")
    sys.stderr.flush()
    sys.exit(1)

if __name__ == "__main__":
    main()

