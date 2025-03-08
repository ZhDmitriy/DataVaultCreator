from setuptools import setup, find_packages
from pathlib import Path
import os

dist_dir = "setup_f"
bdist_dir = "build_f"

os.makedirs(dist_dir, exist_ok=True)
os.makedirs(bdist_dir, exist_ok=True)

def read_requirements():
    requirements_path = Path(__file__).parent / "requirements.txt"
    with open(requirements_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

setup(
    name="datavaulcreator",  
    version="1",      
    author="Dmitriy Zhdanov",  
    author_email="zhdanovwork@yandex.ru", 
    description="Create Data Vault model framework",
    packages=find_packages(),  
    install_requires=read_requirements(),
    python_requires=">=3.6",
)