"""Blender Cycles GPU render of the factory scene for the cross-machine GPU check.

Run as: blender -b --factory-startup --python render.py
Writes render.png into the current directory (the task sandbox). Exits 3 when
no OptiX or CUDA device is found, so the task fails instead of using the CPU.
"""

import os
import sys
import time

import bpy

prefs = bpy.context.preferences.addons["cycles"].preferences
chosen = None
for kind in ("OPTIX", "CUDA"):
    prefs.compute_device_type = kind
    prefs.get_devices()
    gpus = [d for d in prefs.devices if d.type == kind]
    if gpus:
        chosen = kind
        for d in prefs.devices:
            d.use = d.type == kind
        break
if chosen is None:
    print("NO_GPU_DEVICE", [(d.name, d.type) for d in prefs.devices])
    sys.exit(3)

scene = bpy.context.scene
scene.render.engine = "CYCLES"
scene.cycles.device = "GPU"
scene.cycles.samples = 256
scene.render.resolution_x = 1920
scene.render.resolution_y = 1080
scene.render.resolution_percentage = 100
scene.render.image_settings.file_format = "PNG"
scene.render.filepath = os.path.join(os.getcwd(), "render.png")

print("blender", bpy.app.version_string, "backend", chosen,
      "devices", [d.name for d in prefs.devices if d.use])
t0 = time.perf_counter()
bpy.ops.render.render(write_still=True)
print(f"render_ms {(time.perf_counter() - t0) * 1000:.0f} size 1920x1080 samples 256")
print("BLENDER_CYCLES_GPU_OK")
