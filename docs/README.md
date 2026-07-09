# Insula Docs

This repository contains the documentation for Insula, written in [reStructuredText](https://en.wikipedia.org/wiki/ReStructuredText) format.

## Requirements

### Development Requirements

To set up a working development environment for Insula, ensure you have the following software requirements:

- **Python**
- **Python Virtual Environment Tool**

### Tested Environment

The documentation has been locally tested with the following environment specifications:

- **Operating System**: Ubuntu
- **Architecture**: x86_64 (64-bit)
- **Python**: 3.10.12
- **Virtual Environment Tool**: `python3.10-venv`

## Getting Started

- Install Sphinx and the required theme as a pip package. Run the following commands:

```bash
python3 -m venv /path/to/sphinx-env # See https://docs.python.org/3/library/venv.html
source /path/to/sphinx-env/bin/activate
pip3 install sphinx==8.1.3 pydata-sphinx-theme==0.16.1
```

- Clone the repository:

```bash
git clone https://github.com/SercoSPA/cyberitaly-documentation.git
cd docs
```

- Build the documentation. Run:

```bash
make html
```

- You should see output similar to the following:

```bash
Running Sphinx v7.4.7
loading translations [en]... done
loading pickled environment... done
building [mo]: targets for 0 po files that are out of date
writing output...
building [html]: targets for 1 source files that are out of date
updating environment: 0 added, 1 changed, 0 removed
reading sources... [100%] lab/python_api
looking for now-outdated files... none found
pickling environment... done
checking consistency... done
preparing documents... done
copying assets...
copying static files... done
copying extra files... done
copying assets: done
writing output... [100%] lab/python_api
generating indices... genindex done
writing additional pages... search done
copying images... [100%] lab/images/python_api_new_package.png
dumping search index in English (code: en)... done
dumping object inventory... done
build succeeded, 2 warnings.

The HTML pages are in build/html.
```

- Open the file *build/html/index.html*.

To view the documentation, open the file build/html/index.html in a web browser. Navigate to the file in your file explorer, right-click, and select "Open with" followed by your preferred browser.

## Local developing while watching for changes

During development, you can use the sphinx-autobuild tool to automatically rebuild the project and reload the browser whenever changes are made. This eliminates the need to manually run make html after every change.

- Install sphinx-autobuild:

```bash
pip install sphinx-autobuild
```

- Start the autobuild process:

From the project's root folder, run:

```bash
sphinx-autobuild source build/html
```

The command will start a local server (default: <http://127.0.0.1:8000>) and automatically watch for changes.

Open your browser and navigate to the URL shown in the terminal (e.g., <http://127.0.0.1:8000>). As you make changes to your source files, the documentation will rebuild, and the browser will refresh automatically.

Manually rebuilding the entire build directory is sometimes necessary, as sphinx-autobuild is unable to fully rebuild the DOM to include the changed files.

In this case, run:

```bash
make clean

make html
```

## Publish

The documentation is published on GitHub at <https://github.com/SercoSPA/cyberitaly-documentation.git>.

**Note:** Ensure to test the HTML rendering locally, as explained in the [Getting Started](#getting-started) section.
