from setuptools import setup, find_packages

setup(
    name="bambu-mqtt-comms",
    version="0.1.0",
    description="MQTT communication library for Bambu Lab printers",
    author="",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "paho-mqtt>=2.0.0",
    ],
    extras_require={
        "dev": ["pytest", "pytest-asyncio"],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)