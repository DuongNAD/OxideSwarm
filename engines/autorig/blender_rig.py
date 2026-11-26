#!/usr/bin/env python3
"""
OxideSwarm Blender 4.5.4 LTS Headless Auto-Rigging Driver
File: engines/autorig/blender_rig.py
"""

import argparse
import math
import os
import sys

VALID_PRESETS = ("biped", "quadruped", "generic")
VALID_CLIPS = ("idle", "walk", "turnaround")


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
    parser.add_argument("--preset", type=str, default="biped", help="Rig preset (biped, quadruped, generic)")
    parser.add_argument("--animations", type=str, default="idle,walk,turnaround", help="Comma-separated clips")
    return parser.parse_args(argv)


def validate_options(preset_raw, animations_raw):
    preset = preset_raw.strip().lower()
    if preset not in VALID_PRESETS:
        raise ValueError(
            f"Unsupported AutoRig rig_preset '{preset_raw}'; valid presets are: {', '.join(VALID_PRESETS)}"
        )

    requested_clips = []
    if animations_raw.strip():
        for raw_clip in animations_raw.split(","):
            clip = raw_clip.strip().lower()
            if not clip or clip not in VALID_CLIPS:
                raise ValueError(
                    f"Unsupported AutoRig animation clip '{raw_clip}'; valid clips are: {', '.join(VALID_CLIPS)}"
                )
            if clip in requested_clips:
                raise ValueError(
                    f"Duplicate AutoRig animation clip '{clip}' in --animations"
                )
            requested_clips.append(clip)
    return preset, requested_clips


def build_biped_bones(edit_bones, min_z, height, width, mid_x, mid_y):
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

    hip_width = width * 0.11
    knee_prebend = height * 0.02

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

    half_shoulder = width * 0.22
    elbow_x = width * 0.38
    wrist_x = width * 0.46
    elbow_prebend = height * 0.02

    l_arm = edit_bones.new("UpperArm_L")
    l_arm.head = (mid_x - half_shoulder, mid_y, min_z + height * 0.74)
    l_arm.tail = (mid_x - elbow_x, mid_y + elbow_prebend, min_z + height * 0.58)
    l_arm.parent = chest

    l_forearm = edit_bones.new("LowerArm_L")
    l_forearm.head = l_arm.tail
    l_forearm.tail = (mid_x - wrist_x, mid_y, min_z + height * 0.48)
    l_forearm.parent = l_arm

    r_arm = edit_bones.new("UpperArm_R")
    r_arm.head = (mid_x + half_shoulder, mid_y, min_z + height * 0.74)
    r_arm.tail = (mid_x + elbow_x, mid_y + elbow_prebend, min_z + height * 0.58)
    r_arm.parent = chest

    r_forearm = edit_bones.new("LowerArm_R")
    r_forearm.head = r_arm.tail
    r_forearm.tail = (mid_x + wrist_x, mid_y, min_z + height * 0.48)
    r_forearm.parent = r_arm


def build_quadruped_bones(edit_bones, min_y, min_z, height, width, depth, mid_x):
    # In Blender: Z is Up, Y is Longitudinal/Forward (+Y forward, -Y tail), X is Lateral
    pelvis = edit_bones.new("Pelvis")
    pelvis.head = (mid_x, min_y + depth * 0.25, min_z + height * 0.56)
    pelvis.tail = (mid_x, min_y + depth * 0.44, min_z + height * 0.60)

    spine = edit_bones.new("Spine")
    spine.head = pelvis.tail
    spine.tail = (mid_x, min_y + depth * 0.65, min_z + height * 0.62)
    spine.parent = pelvis

    chest = edit_bones.new("Chest")
    chest.head = spine.tail
    chest.tail = (mid_x, min_y + depth * 0.78, min_z + height * 0.74)
    chest.parent = spine

    neck = edit_bones.new("Neck")
    neck.head = chest.tail
    neck.tail = (mid_x, min_y + depth * 0.88, min_z + height * 0.88)
    neck.parent = chest

    head = edit_bones.new("Head")
    head.head = neck.tail
    head.tail = (mid_x, min_y + depth * 1.00, min_z + height * 0.88)
    head.parent = neck

    tail_01 = edit_bones.new("Tail_01")
    tail_01.head = (mid_x, min_y + depth * 0.16, min_z + height * 0.58)
    tail_01.tail = (mid_x, min_y + depth * 0.08, min_z + height * 0.53)
    tail_01.parent = pelvis

    tail_02 = edit_bones.new("Tail_02")
    tail_02.head = tail_01.tail
    tail_02.tail = (mid_x, min_y + depth * 0.01, min_z + height * 0.45)
    tail_02.parent = tail_01

    leg_x = max(width * 0.18, height * 0.05)
    y_hind = min_y + depth * 0.24
    y_front = min_y + depth * 0.70

    for suffix, sign in (("L", -1.0), ("R", 1.0)):
        x = mid_x + sign * leg_x

        h_up = edit_bones.new(f"HindUpperLeg_{suffix}")
        h_up.head = (x, y_hind, min_z + height * 0.54)
        h_up.tail = (x, y_hind - depth * 0.04, min_z + height * 0.30)
        h_up.parent = pelvis

        h_lo = edit_bones.new(f"HindLowerLeg_{suffix}")
        h_lo.head = h_up.tail
        h_lo.tail = (x, y_hind + depth * 0.02, min_z + height * 0.06)
        h_lo.parent = h_up

        h_paw = edit_bones.new(f"HindPaw_{suffix}")
        h_paw.head = h_lo.tail
        h_paw.tail = (x, y_hind + depth * 0.08, min_z)
        h_paw.parent = h_lo

        f_up = edit_bones.new(f"FrontUpperLeg_{suffix}")
        f_up.head = (x, y_front, min_z + height * 0.56)
        f_up.tail = (x, y_front - depth * 0.03, min_z + height * 0.31)
        f_up.parent = chest

        f_lo = edit_bones.new(f"FrontLowerLeg_{suffix}")
        f_lo.head = f_up.tail
        f_lo.tail = (x, y_front + depth * 0.02, min_z + height * 0.06)
        f_lo.parent = f_up

        f_paw = edit_bones.new(f"FrontPaw_{suffix}")
        f_paw.head = f_lo.tail
        f_paw.tail = (x, y_front + depth * 0.08, min_z)
        f_paw.parent = f_lo


def build_generic_bones(edit_bones, min_z, height, mid_x, mid_y):
    root = edit_bones.new("Root")
    root.head = (mid_x, mid_y, min_z + height * 0.15)
    root.tail = (mid_x, mid_y, min_z + height * 0.40)

    spine = edit_bones.new("Spine")
    spine.head = root.tail
    spine.tail = (mid_x, mid_y, min_z + height * 0.62)
    spine.parent = root

    chest = edit_bones.new("Chest")
    chest.head = spine.tail
    chest.tail = (mid_x, mid_y, min_z + height * 0.80)
    chest.parent = spine

    neck = edit_bones.new("Neck")
    neck.head = chest.tail
    neck.tail = (mid_x, mid_y, min_z + height * 0.88)
    neck.parent = chest

    head = edit_bones.new("Head")
    head.head = neck.tail
    head.tail = (mid_x, mid_y, min_z + height * 1.00)
    head.parent = neck


def synthesize_clip_keyframes(armature_obj, preset, clip_name, height, fps=30):
    root_name = {
        "biped": "Hips",
        "quadruped": "Pelvis",
        "generic": "Root",
    }[preset]

    if clip_name == "idle":
        frames = 73
        omega = 2.0 * math.pi / (frames / fps)
        for f in range(frames):
            t = f / fps
            root_bone = armature_obj.pose.bones.get(root_name)
            if root_bone:
                root_bone.location = (0.0, 0.0, -0.006 * height * math.sin(omega * t))
                root_bone.keyframe_insert(data_path="location", frame=f + 1)
            chest_bone = armature_obj.pose.bones.get("Chest")
            if chest_bone:
                chest_bone.rotation_mode = 'XYZ'
                chest_bone.rotation_euler = (0.035 * math.sin(omega * t), 0.0, 0.0)
                chest_bone.keyframe_insert(data_path="rotation_euler", frame=f + 1)
            if preset == "quadruped":
                tail_bone = armature_obj.pose.bones.get("Tail_01")
                if tail_bone:
                    tail_bone.rotation_mode = 'XYZ'
                    tail_bone.rotation_euler = (0.0, 0.0, 0.10 * math.sin(omega * t))
                    tail_bone.keyframe_insert(data_path="rotation_euler", frame=f + 1)
    elif clip_name == "walk":
        frames = 37
        two_pi = 2.0 * math.pi
        for f in range(frames):
            t = f / fps
            phi = two_pi * (t / (frames / fps))
            root_bone = armature_obj.pose.bones.get(root_name)
            if root_bone:
                root_bone.location = (0.0, 0.0, -0.018 * height * (1.0 - math.cos(2.0 * phi)))
                root_bone.keyframe_insert(data_path="location", frame=f + 1)
            if preset == "biped":
                for bone_name, phase in (("UpperLeg_L", phi), ("UpperLeg_R", phi + math.pi)):
                    bone = armature_obj.pose.bones.get(bone_name)
                    if bone:
                        bone.rotation_mode = 'XYZ'
                        bone.rotation_euler = (0.38 * math.sin(phase), 0.0, 0.0)
                        bone.keyframe_insert(data_path="rotation_euler", frame=f + 1)
            elif preset == "quadruped":
                for bone_name, phase in (
                    ("HindUpperLeg_L", phi),
                    ("FrontUpperLeg_R", phi),
                    ("HindUpperLeg_R", phi + math.pi),
                    ("FrontUpperLeg_L", phi + math.pi),
                ):
                    bone = armature_obj.pose.bones.get(bone_name)
                    if bone:
                        bone.rotation_mode = 'XYZ'
                        bone.rotation_euler = (0.34 * math.sin(phase), 0.0, 0.0)
                        bone.keyframe_insert(data_path="rotation_euler", frame=f + 1)
                tail_bone = armature_obj.pose.bones.get("Tail_01")
                if tail_bone:
                    tail_bone.rotation_mode = 'XYZ'
                    tail_bone.rotation_euler = (0.0, 0.0, 0.18 * math.sin(phi))
                    tail_bone.keyframe_insert(data_path="rotation_euler", frame=f + 1)
            else:
                spine_bone = armature_obj.pose.bones.get("Spine")
                if spine_bone:
                    spine_bone.rotation_mode = 'XYZ'
                    spine_bone.rotation_euler = (0.025, 0.0, 0.04 * math.sin(phi))
                    spine_bone.keyframe_insert(data_path="rotation_euler", frame=f + 1)
    elif clip_name == "turnaround":
        frames = 97
        duration = frames / fps
        two_pi = 2.0 * math.pi
        for f in range(frames):
            t = f / fps
            u = t / duration
            yaw = two_pi * (u - (0.12 / two_pi) * math.sin(8.0 * math.pi * u))
            dz = -0.015 * height * abs(math.sin(4.0 * math.pi * u))
            root_bone = armature_obj.pose.bones.get(root_name)
            if root_bone:
                root_bone.location = (0.0, 0.0, dz)
                root_bone.keyframe_insert(data_path="location", frame=f + 1)
                root_bone.rotation_mode = 'XYZ'
                root_bone.rotation_euler = (0.0, 0.0, yaw)
                root_bone.keyframe_insert(data_path="rotation_euler", frame=f + 1)

            step_phase = (t % 0.8) / 0.8
            if step_phase < 0.5:
                lift_r = math.sin(math.pi * (step_phase / 0.5)) * 0.40
                lift_l = 0.0
            else:
                lift_r = 0.0
                lift_l = math.sin(math.pi * ((step_phase - 0.5) / 0.5)) * 0.40

            if preset == "biped":
                for bone_name, lift in (("UpperLeg_L", lift_l), ("UpperLeg_R", lift_r)):
                    bone = armature_obj.pose.bones.get(bone_name)
                    if bone:
                        bone.rotation_mode = 'XYZ'
                        bone.rotation_euler = (lift * 0.6, 0.0, 0.0)
                        bone.keyframe_insert(data_path="rotation_euler", frame=f + 1)
            elif preset == "quadruped":
                for bone_name, lift in (
                    ("HindUpperLeg_L", lift_l),
                    ("FrontUpperLeg_R", lift_l),
                    ("HindUpperLeg_R", lift_r),
                    ("FrontUpperLeg_L", lift_r),
                ):
                    bone = armature_obj.pose.bones.get(bone_name)
                    if bone:
                        bone.rotation_mode = 'XYZ'
                        bone.rotation_euler = (lift * 0.5, 0.0, 0.0)
                        bone.keyframe_insert(data_path="rotation_euler", frame=f + 1)


def main():
    args = parse_args()
    preset, requested_clips = validate_options(args.preset, args.animations)

    import bpy
    import mathutils

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
    depth = max(max_y - min_y, 1e-4)
    mid_x = (min_x + max_x) * 0.5
    mid_y = (min_y + max_y) * 0.5

    # 4. Construct Preset-Specific Standard Armature
    print(f"[PROGRESS: 0.45] Synthesizing {preset} armature...", flush=True)
    armature_data = bpy.data.armatures.new("StandardArmature")
    armature_obj = bpy.data.objects.new("Armature", armature_data)
    bpy.context.scene.collection.objects.link(armature_obj)
    bpy.context.view_layer.objects.active = armature_obj
    armature_obj.select_set(True)

    bpy.ops.object.mode_set(mode='EDIT')
    edit_bones = armature_data.edit_bones
    if preset == "biped":
        build_biped_bones(edit_bones, min_z, height, width, mid_x, mid_y)
    elif preset == "quadruped":
        build_quadruped_bones(edit_bones, min_y, min_z, height, width, depth, mid_x)
    else:
        build_generic_bones(edit_bones, min_z, height, mid_x, mid_y)
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
    for clip_name in requested_clips:
        action = bpy.data.actions.new(name=clip_name)
        armature_obj.animation_data_create()
        armature_obj.animation_data.action = action
        synthesize_clip_keyframes(armature_obj, preset, clip_name, height, fps=30)

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
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
