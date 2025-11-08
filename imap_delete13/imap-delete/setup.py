from setuptools import setup, find_packages

setup(
    name="imap-delete",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[],
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "imap-delete=imap_delete.main:main",
        ],
    },
)
