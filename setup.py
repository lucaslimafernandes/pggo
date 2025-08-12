# setup.py
import os, sys, subprocess
from pathlib import Path
from setuptools import setup
from setuptools.command.build_py import build_py as _build_py

ROOT = Path(__file__).parent.resolve()
GO_DIR = ROOT / "go"
PKG_DIR = ROOT / "pggo"

def out_name():
    if sys.platform.startswith("win"):
        return "pggo.dll"
    elif sys.platform == "darwin":
        return "libpggo.dylib"
    else:
        return "libpggo.so"

class build_py(_build_py):
    def run(self):
        # compilar a lib Go para dentro do pacote
        if not GO_DIR.exists():
            raise FileNotFoundError(f"Pasta Go não encontrada: {GO_DIR}")
        subprocess.check_call(["go", "mod", "download"], cwd=str(GO_DIR))
        out = PKG_DIR / out_name()
        env = os.environ.copy()
        env["CGO_ENABLED"] = "1"
        subprocess.check_call(
            ["go", "build", "-buildmode=c-shared", "-o", str(out)],
            cwd=str(GO_DIR),
            env=env,
        )
        super().run()

setup(
    name="pggo",
    version="0.1.0",
    description="Driver Postgres em Go exposto para Python (MVP)",
    packages=["pggo"],
    include_package_data=True,
    package_data={"pggo": ["libpggo.so", "libpggo.dylib", "pggo.dll"]},
    python_requires=">=3.9",
    cmdclass={"build_py": build_py},
)
