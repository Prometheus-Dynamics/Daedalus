#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples/08_ffi"


def load_json(path: Path) -> dict | list:
    return json.loads(path.read_text(encoding="utf-8"))


def rust_baseline() -> dict[str, dict]:
    # This mirrors the Rust showcase declarations in examples/08_ffi/rust/complex_plugin/src/lib.rs.
    return {
        "scalar_add": surface(["a", "b"], ["out"]),
        "split_sign": surface(["value"], ["positive", "negative"]),
        "scale": surface(["value", "config"], ["out"]),
        "accumulate": surface(["value"], ["sum"], stateful=True),
        "bytes_len": surface(["payload"], ["len"]),
        "image_boost": surface(["rgba8"], ["rgba8"]),
        "shape_summary": surface(
            ["point", "mode", "maybe", "items", "labels", "pair", "unit"],
            ["summary"],
        ),
        "emit_event": surface(["message"], ["ok"]),
        "capability_add": surface(["a", "b"], ["out"]),
        "checked_divide": surface(["a", "b"], ["out"]),
        "array_dynamic_sum": surface(["values"], ["sum"]),
        "node_io_complex": surface(["point", "weights", "metadata"], ["score", "label", "point"]),
        "gpu_tint": surface(["rgba8"], ["rgba8"]),
        "internal_adapter_consume": surface(["count"], ["out"]),
        "external_adapter_consume": surface(["count"], ["out"]),
        "zero_copy_len": surface(["frame"], ["len"]),
        "shared_ref_len": surface(["frame"], ["len"]),
        "cow_append_marker": surface(["frame"], ["frame"]),
        "mutable_brighten": surface(["rgba8"], ["rgba8"]),
        "owned_bytes_len": surface(["blob"], ["len"]),
    }


def surface(inputs: list[str], outputs: list[str], stateful: bool = False) -> dict:
    return {
        "inputs": sorted(inputs),
        "outputs": sorted(outputs),
        "stateful": stateful,
    }


def descriptor_surface(descriptor: dict) -> dict[str, dict]:
    return {
        node["id"]: {
            "inputs": sorted(port["name"] for port in node.get("inputs", [])),
            "outputs": sorted(port["name"] for port in node.get("outputs", [])),
            "stateful": bool(node.get("stateful")),
        }
        for node in descriptor["schema"]["nodes"]
    }


def python_descriptor() -> dict:
    script = f"""
import json
import sys
sys.path.insert(0, {str(ROOT / 'crates/ffi/python/sdk')!r})
sys.path.insert(0, {str(EXAMPLES / 'python/complex_plugin')!r})
from ffi_showcase import showcase_plugin
print(json.dumps(showcase_plugin.descriptor(), sort_keys=True))
"""
    output = subprocess.check_output([sys.executable, "-c", script], cwd=ROOT, text=True)
    return json.loads(output)


def node_descriptor() -> dict:
    script = """
import { showcasePlugin } from './examples/08_ffi/node/complex_plugin/src/plugin.ts';
console.log(JSON.stringify(showcasePlugin.descriptor()));
"""
    output = subprocess.check_output(
        ["node", "--input-type=module", "-"],
        cwd=ROOT,
        input=script,
        text=True,
    )
    return json.loads(output)


def java_descriptor() -> dict:
    with tempfile.TemporaryDirectory(prefix="daedalus-ffi-java-sdk-") as temp:
        temp_path = Path(temp)
        classes = temp_path / "classes"
        sources = [
            *sorted((ROOT / "crates/ffi/java/sdk/src/main/java").rglob("*.java")),
            *sorted((EXAMPLES / "java/complex_plugin/src/main/java").rglob("*.java")),
            EXAMPLES / "java/complex_plugin/build-package.java",
        ]
        subprocess.check_call(["javac", "-d", str(classes), *map(str, sources)], cwd=ROOT)
        subprocess.check_call(["java", "-cp", str(classes), "BuildPackage"], cwd=temp_path)
        return load_json(temp_path / "plugin.json")


def cpp_descriptor() -> dict:
    with tempfile.TemporaryDirectory(prefix="daedalus-ffi-cpp-sdk-") as temp:
        temp_path = Path(temp)
        binary = temp_path / "build-package"
        subprocess.check_call(
            [
                "c++",
                "-std=c++20",
                f"-I{ROOT / 'crates/ffi/cpp/sdk/include'}",
                f"-I{EXAMPLES / 'cpp/complex_plugin'}",
                str(EXAMPLES / "cpp/complex_plugin/build-package.cpp"),
                "-o",
                str(binary),
            ],
            cwd=ROOT,
        )
        subprocess.check_call([str(binary)], cwd=temp_path)
        return load_json(temp_path / "plugin.json")


class CrossLanguageDescriptorSnapshots(unittest.TestCase):
    maxDiff = None

    def test_showcase_sdk_descriptors_match_rust_baseline_surface(self) -> None:
        baseline = rust_baseline()
        descriptors = {
            "python": python_descriptor(),
            "node": node_descriptor(),
            "java": java_descriptor(),
            "c_cpp": cpp_descriptor(),
        }

        for language, descriptor in descriptors.items():
            with self.subTest(language=language):
                self.assertEqual(descriptor["schema_version"], 1)
                self.assertEqual(descriptor["metadata"]["language"], language)
                self.assertEqual(descriptor_surface(descriptor), baseline)
                self.assertEqual(set(descriptor["backends"]), set(baseline))
                self.assertFeatureMarkers(language, descriptor)

    def assertFeatureMarkers(self, language: str, descriptor: dict) -> None:
        nodes = {node["id"]: node for node in descriptor["schema"]["nodes"]}
        self.assertTrue(nodes["accumulate"]["stateful"])
        self.assertEqual(nodes["gpu_tint"]["inputs"][0].get("residency"), "gpu")
        self.assertEqual(nodes["gpu_tint"]["inputs"][0].get("layout"), "rgba8-hwc")
        self.assertEqual(nodes["zero_copy_len"]["inputs"][0].get("access"), "view")
        self.assertEqual(nodes["cow_append_marker"]["inputs"][0].get("access"), "modify")
        self.assertEqual(nodes["owned_bytes_len"]["inputs"][0].get("access"), "move")
        self.assertEqual(
            nodes["capability_add"]["metadata"].get("capability"),
            "Add",
            f"{language} descriptor missing capability metadata",
        )
        self.assertEqual(
            descriptor["schema"]["boundary_contracts"][0]["type_key"],
            "ffi.showcase.Point",
        )
        self.assertEqual(
            sorted(descriptor["metadata"].get("adapters", [])),
            [
                "ffi.showcase.external_legacy_count_to_i64",
                "ffi.showcase.internal_count_to_i64",
            ],
        )


if __name__ == "__main__":
    missing = [tool for tool in ("node", "javac", "java", "c++") if shutil.which(tool) is None]
    if missing:
        raise SystemExit(f"missing required descriptor snapshot tools: {', '.join(missing)}")
    unittest.main()
