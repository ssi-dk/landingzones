"""Check the disposable lab's real CLI configuration without requiring Docker."""
import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest
import yaml


LAB = Path(__file__).parent / "container_lab"
spec = importlib.util.spec_from_file_location("container_lab_node", LAB / "node.py")
node = importlib.util.module_from_spec(spec)
spec.loader.exec_module(node)


@pytest.mark.parametrize("role,user,count", [
    ("cluster-a", "a_transfer", 4),
    ("cluster-a", "a_distributor", 2),
    ("cluster-b", "b_distributor", 2),
])
def test_real_cli_builds_lab_runtimes(tmp_path, role, user, count):
    node.write_config(role, user, tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "landingzones.cli", "--config", str(tmp_path / "config.yaml"), "build"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    scripts = list((tmp_path / "output/scripts").glob("*.sh"))
    assert len(scripts) == count, result.stdout
    for row in node.routes(role, user):
        path = tmp_path / "output/scripts" / (row["identifiers"] + ".sh")
        content = path.read_text()
        assert str(tmp_path / "log") in content
        assert str(tmp_path / "flock") in content
        assert row["runtime_id"] in content
        subprocess.run(["sh", "-n", str(path)], check=True)


def test_compose_uses_one_image_and_separate_filesystems():
    compose = yaml.safe_load((LAB / "compose.yaml").read_text())
    services = compose["services"]
    assert set(services) == {"lab", "cluster-a", "cluster-b"}
    assert len({s["image"] for s in services.values()}) == 1
    assert compose["networks"]["lab"]["internal"] is True
    for service in services.values():
        assert "volumes" not in service
        assert "ports" not in service
        assert not service.get("privileged", False)


def test_lab_driver_parses():
    result = subprocess.run([sys.executable, str(LAB / "lab.py"), "--help"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "setup,run,inspect,reset" in result.stdout
