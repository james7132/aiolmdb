import setuptools

with open('README.md', 'r') as f:
    long_description = f.read()

setuptools.setup(
    name="aiolmdb",
    version="0.1.1",
    author="James Liu",
    author_email="contact@jamessliu.com",
    description="An asyncio wrapper around Lighting Memory Mapped Database (LMDB)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/james7132/aiolmdb",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
