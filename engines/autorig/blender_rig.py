#!/usr/bin/env python3
"""
OxideSwarm Blender 4.5.4 LTS Headless Auto-Rigging Driver
File: engines/autorig/blender_rig.py
"""

import argparse
import math
import os
import sys

def parse_args():
    # Detect the '--' separator in sys.argv passed by Blender
    argv = sys.argv
    if "--" in argv:
        argv = argv[argv.index("--") + 1:]
    else:
        argv = []

    parser = argparse.ArgumentParser(description="Blender Headless Auto-Rigging")
    parser.add_argument("--input", type=str, required=True, help="Input mesh file path")
    parser.add_argument("--output", type=str, required=True, help="Output .glb path")
    parser.add_argument("--preset", type=str, default="biped", help="Rig preset")
    parser.add_argument("--animations", type=str, default="idle,walk,turnaround", help="Comma-separated clips")
    return parser.parse_args(argv)

def main():
    import bpy
    import mathutils

    args = parse_args()
    print("[PROGRESS: 0.05] Initializing Blender headless auto-rig session...", flush=True)

    # 1. Reset scene to factory empty
    bpy.ops.wm.read_factory_settings(use_empty=True)

    # 2. Import input mesh
    print(f"[PROGRESS: 0.15] Importing input mesh from {args.input}...", flush=True)
    in_lower = args.input.lower()
    if in_lower.endswith(".glb") or in_lower.endswith(".gltf"):
        bpy.ops.import_scene.gltf(filepath=args.input)
    elif in_lower.endswith(".obj"):
        if hasattr(bpy.ops.wm, "obj_import"):
            bpy.ops.wm.obj_import(filepath=args.input)
        else:
            bpy.ops.import_scene.obj(filepath=args.input)
    else:
        raise ValueError(f"Unsupported mesh format: {args.input}")

    mesh_objs = [obj for obj in bpy.context.scene.objects if obj.type == 'MESH']
    if not mesh_objs:
        raise RuntimeError("No mesh object found in imported file")

    # If multiple mesh pieces, join into a single mesh
    bpy.ops.object.select_all(action='DESELECT')
    for obj in mesh_objs:
        obj.select_set(True)
    bpy.context.view_layer.objects.active = mesh_objs[0]
    if len(mesh_objs) > 1:
        bpy.ops.object.join()
    mesh = bpy.context.view_layer.objects.active
    mesh.name = "CharacterMesh"

    # 3. Compute Bounding Box Landmarks
    print("[PROGRESS: 0.30] Computing mesh bounding box landmarks...", flush=True)
    bbox_corners = [mesh.matrix_world @ mathutils.Vector(corner) for corner in mesh.bound_box]
    min_x = min(c.x for c in bbox_corners)
    max_x = max(c.x for c in bbox_corners)
    min_y = min(c.y for c in bbox_corners)
    max_y = max(c.y for c in bbox_corners)
    min_z = min(c.z for c in bbox_corners)
    max_z = max(c.z for c in bbox_corners)

    # In Blender, Z is Up, Y is Depth, X is Lateral
    height = max(max_z - min_z, 1e-4)
    width = max(max_x - min_x, 1e-4)
    mid_x = (min_x + max_x) * 0.5
    mid_y = (min_y + max_y) * 0.5

    # 4. Construct 15-Joint Standard Armature
    print("[PROGRESS: 0.45] Synthesizing 15-joint standard armature...", flush=True)
    armature_data = bpy.data.armatures.new("StandardArmature")
    armature_obj = bpy.data.objects.new("Armature", armature_data)
    bpy.context.scene.collection.objects.link(armature_obj)
    bpy.context.view_layer.objects.active = armature_obj
    armature_obj.select_set(True)

    bpy.ops.object.mode_set(mode='EDIT')
    edit_bones = armature_data.edit_bones

    # Axial Bones (Hips, Spine, Chest, Neck, Head)
    hips = edit_bones.new("Hips")
    hips.head = (mid_x, mid_y, min_z + height * 0.52)
    hips.tail = (mid_x, mid_y, min_z + height * 0.62)

    spine = edit_bones.new("Spine")
    spine.head = hips.tail
    spine.tail = (mid_x, mid_y, min_z + height * 0.74)
    spine.parent = hips

    chest = edit_bones.new("Chest")
    chest.head = spine.tail
    chest.tail = (mid_x, mid_y, min_z + height * 0.83)
    chest.parent = spine

    neck = edit_bones.new("Neck")
    neck.head = chest.tail
    neck.tail = (mid_x, mid_y, min_z + height * 0.88)
    neck.parent = chest

    head = edit_bones.new("Head")
    head.head = neck.tail
    head.tail = (mid_x, mid_y, min_z + height * 1.00)
    head.parent = neck

    # Left & Right Legs
    hip_width = width * 0.11
    knee_prebend = height * 0.02

    # Left Leg (X < 0 in Blender or X > 0 depending on view; let's use -X for Left, +X for Right)
    l_thigh = edit_bones.new("UpperLeg_L")
    l_thigh.head = (mid_x - hip_width, mid_y, min_z + height * 0.50)
    l_thigh.tail = (mid_x - hip_width, mid_y - knee_prebend, min_z + height * 0.26)
    l_thigh.parent = hips

    l_shin = edit_bones.new("LowerLeg_L")
    l_shin.head = l_thigh.tail
    l_shin.tail = (mid_x - hip_width, mid_y, min_z + height * 0.06)
    l_shin.parent = l_thigh

    l_foot = edit_bones.new("Foot_L")
    l_foot.head = l_shin.tail
    l_foot.tail = (mid_x - hip_width, mid_y - height * 0.08, min_z)
    l_foot.parent = l_shin

    # Right Leg
    r_thigh = edit_bones.new("UpperLeg_R")
    r_thigh.head = (mid_x + hip_width, mid_y, min_z + height * 0.50)
    r_thigh.tail = (mid_x + hip_width, mid_y - knee_prebend, min_z + height * 0.26)
    r_thigh.parent = hips

    r_shin = edit_bones.new("LowerLeg_R")
    r_shin.head = r_thigh.tail
    r_shin.tail = (mid_x + hip_width, mid_y, min_z + height * 0.06)
    r_shin.parent = r_thigh

    r_foot = edit_bones.new("Foot_R")
    r_foot.head = r_shin.tail
    r_foot.tail = (mid_x + hip_width, mid_y - height * 0.08, min_z)
    r_foot.parent = r_shin

    # Left & Right Arms
    half_shoulder = width * 0.22
    elbow_x = width * 0.38
    wrist_x = width * 0.46
    elbow_prebend = height * 0.02

    # Left Arm
    l_arm = edit_bones.new("UpperArm_L")
    l_arm.head = (mid_x - half_shoulder, mid_y, min_z + height * 0.74)
    l_arm.tail = (mid_x - elbow_x, mid_y + elbow_prebend, min_z + height * 0.58)
    l_arm.parent = chest

    l_forearm = edit_bones.new("LowerArm_L")
    l_forearm.head = l_arm.tail
    l_forearm.tail = (mid_x - wrist_x, mid_y, min_z + height * 0.48)
    l_forearm.parent = l_arm

    # Right Arm
    r_arm = edit_bones.new("UpperArm_R")
    r_arm.head = (mid_x + half_shoulder, mid_y, min_z + height * 0.74)
    r_arm.tail = (mid_x + elbow_x, mid_y + elbow_prebend, min_z + height * 0.58)
    r_arm.parent = chest

    r_forearm = edit_bones.new("LowerArm_R")
    r_forearm.head = r_arm.tail
    r_forearm.tail = (mid_x + wrist_x, mid_y, min_z + height * 0.48)
    r_forearm.parent = r_arm

    bpy.ops.object.mode_set(mode='OBJECT')

    # 5. Automatic Skin Weighting (Heat Diffusion Solver)
    print("[PROGRESS: 0.65] Binding mesh to armature via automatic heat weights...", flush=True)
    bpy.ops.object.select_all(action='DESELECT')
    mesh.select_set(True)
    armature_obj.select_set(True)
    bpy.context.view_layer.objects.active = armature_obj

    try:
        bpy.ops.object.parent_set(type='ARMATURE_AUTO')
    except Exception as e:
        print(f"[PROGRESS: 0.70] Heat weighting failed ({e}), falling back to envelope weighting", flush=True)
        bpy.ops.object.parent_set(type='ARMATURE_ENVELOPE')

    # 6. Procedural Keyframing / Animation Tracks
    print("[PROGRESS: 0.80] Synthesizing requested animation actions...", flush=True)
    requested_clips = [c.strip().lower() for c in args.animations.split(",") if c.strip()]
    fps = 30

    for clip_name in requested_clips:
        action = bpy.data.actions.new(name=clip_name)
        armature_obj.animation_data_create()
        armature_obj.animation_data.action = action

        if clip_name == "idle":
            frames = 73
            omega = 2.0 * math.pi / (frames / fps)
            for f in range(frames):
                t = f / fps
                # Spine / Chest breathing pitch
                p_bone = armature_obj.pose.bones.get("Chest")
                if p_bone:
                    p_bone.rotation_mode = 'XYZ'
                    p_bone.rotation_euler = (0.035 * math.sin(omega * t), 0.0, 0.0)
                    p_bone.keyframe_insert(data_path="rotation_euler", frame=f + 1)
        elif clip_name == "walk":
            frames = 37
            two_pi = 2.0 * math.pi
            for f in range(frames):
                t = f / fps
                phi = two_pi * (t / (frames / fps))
                # Hips bounce & legs swing
                hips_bone = armature_obj.pose.bones.get("Hips")
                if hips_bone:
                    hips_bone.location = (0.0, 0.0, -0.018 * height * (1.0 - math.cos(2.0 * phi)))
                    hips_bone.keyframe_insert(data_path="location", frame=f + 1)
                l_thigh_bone = armature_obj.pose.bones.get("UpperLeg_L")
                if l_thigh_bone:
                    l_thigh_bone.rotation_mode = 'XYZ'
                    l_thigh_bone.rotation_euler = (0.38 * math.sin(phi), 0.0, 0.0)
                    l_thigh_bone.keyframe_insert(data_path="rotation_euler", frame=f + 1)
                r_thigh_bone = armature_obj.pose.bones.get("UpperLeg_R")
                if r_thigh_bone:
                    r_thigh_bone.rotation_mode = 'XYZ'
                    r_thigh_bone.rotation_euler = (0.38 * math.sin(phi + math.pi), 0.0, 0.0)
                    r_thigh_bone.keyframe_insert(data_path="rotation_euler", frame=f + 1)

    # 7. Export glTF 2.0 Binary (.glb)
    print(f"[PROGRESS: 0.95] Exporting rigged glTF asset to {args.output}...", flush=True)
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)

    bpy.ops.export_scene.gltf(
        filepath=args.output,
        export_format='GLB',
        export_skins=True,
        export_influence_nb=4,
        export_all_influences=False,
        export_animations=True,
        export_anim_single_armature=True,
        export_optimize_animation_size=True,
        export_bake_animation=True
    )

    print(f"[PROGRESS: 1.00] Rigged glTF asset exported successfully to {args.output}", flush=True)
    sys.exit(0)

if __name__ == "__main__":
    main()
