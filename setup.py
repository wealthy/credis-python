from setuptools import find_packages, setup

setup(
    name="credis (Redis Controller)",
    packages=find_packages(),
    version="0.1.0",
    description="Redis controller client for Sentinel Setup",
    author="Anshul Patel",
    install_requires=[
        "redis",
        "typing-extensions",
    ],
    python_requires=">=3.9",
)
