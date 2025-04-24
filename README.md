# avail-research-road-network-resiliency

NOTE: This project was initialized using
<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>,
though it since deviated considerably from the template.

```markdown
# Project Setup Instructions

This project uses `pyproject.toml` for dependency management and contains isolated experiment directories. You can use either `uv` or `pip` to install the project and its dependencies.

## Prerequisites

* Python 3.12 or higher.
* (Recommended) A virtual environment manager (either `uv` or `venv`).

## Using `uv` (Recommended for Speed and Efficiency)

1.  **Install `uv` (if not already installed):**

    ```bash
    pip install uv
    ```

2.  **Create and activate a virtual environment (optional but recommended):**

    ```bash
    uv venv .venv
    source .venv/bin/activate  # On macOS/Linux
    .venv\Scripts\activate  # On Windows
    ```

3.  **Install the project in editable mode:**

    ```bash
    uv pip install -e .
    ```

4.  **Verify the installation:**

    ```bash
    uv pip list
    ```

5.  **Sync the virtual environment (if `pyproject.toml` changes):**

    ```bash
    uv pip sync
    ```

6.  **Running Experiments:**

    * Navigate to the experiment directory (e.g., `experiments/experiment_1`).
    * Execute the experiment script using: `python -m bin.run`.

    ```bash
    cd experiments/experiment_1
    python -m bin.run
    ```

7.  **Activating the Virtual Environment:**

    * Whenever you navigate into the project directory, activate the virtual environment:

    ```bash
    source .venv/bin/activate  # On macOS/Linux
    .venv\Scripts\activate  # On Windows
    ```

## Using `pip`

1.  **Create and activate a virtual environment (optional but recommended):**

    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On macOS/Linux
    .venv\Scripts\activate  # On Windows
    ```

2.  **Install the project in editable mode:**

    ```bash
    pip install -e .
    ```

3.  **Verify the installation:**

    ```bash
    pip list
    ```

4.  **Update the virtual environment (if `pyproject.toml` changes):**

    ```bash
    pip install -r requirements.txt # if requirements.txt exists, or:
    pip install -e . # will update the environment based on pyproject.toml
    ```

5.  **Running Experiments:**

    * Navigate to the experiment directory (e.g., `experiments/experiment_1`).
    * Execute the experiment script using: `python -m bin.run`.

    ```bash
    cd experiments/experiment_1
    python -m bin.run
    ```

6.  **Activating the Virtual Environment:**

    * Whenever you navigate into the project directory, activate the virtual environment:

    ```bash
    source .venv/bin/activate  # On macOS/Linux
    .venv\Scripts\activate  # On Windows
    ```

## Important Notes

* **Editable Mode (`-e`):**
    * The `-e` flag (or `--editable`) installs the project in editable mode, allowing you to make changes to the code without reinstalling.
* **Virtual Environments:**
    * Using virtual environments is highly recommended to isolate project dependencies and avoid conflicts with other Python projects.
* **`pyproject.toml`:**
    * All project dependencies are managed in the `pyproject.toml` file.
    * When adding or removing dependencies, run the appropriate sync/update command.
* **Running Experiments (Important):**
    * Experiments are run using `python -m bin.run` from within the experiment directory. This ensures that the relative imports within the experiment's `bin` and `src` directories work correctly.
* **Import Structure:**
    * Remember that the import structure reflects the directory layout within the `common` package. For example: `from common.avail.merged_floodplains import utils`.
* **Experiment Structure:**
    * Each experiment has its own isolated `bin` and `src` directories. The `src` directory contains the core experiment logic, and the `bin` directory contains scripts to run the experiment.
* **Relative Imports in Experiments:**
    * Within experiment directories, relative imports are used (e.g., `from .src.core_logic`). The `bin` directory must be treated as a package, and therefore contain an `__init__.py` file.
* **Activating Virtual Environment:**
    * Always activate the virtual environment when you enter the project directory to ensure you're using the correct dependencies.


## Experiment Directory Structure and Execution

This project includes an `experiments` directory containing isolated experimental code. Each experiment has its own `bin` and `src` directories.

### Directory Structure

Each experiment directory follows this structure:

```
experiments/
└── <experiment_name>/
    ├── bin/
    │   ├── __init__.py  # Marks 'bin' as a package
    │   └── run.py       # Script to run the experiment
    └── src/
        ├── __init__.py  # Marks 'src' as a package
        └── <module>.py   # Core experiment logic
```

* **`bin/`:** Contains executable scripts for running the experiment.
* **`src/`:** Contains the core logic of the experiment, organized into Python modules.
* **`__init__.py`:** These files are crucial. They tell Python that `bin` and `src` are packages, enabling relative imports.

### Running Experiments

To run an experiment, follow these steps:

1.  **Activate the Virtual Environment:**

    * Ensure your virtual environment is activated. This is crucial for using the correct dependencies.

    In the project (not experiment) root directory:

    ```bash
    source .venv/bin/activate  # On macOS/Linux
    .venv\Scripts\activate  # On Windows
    ```

2.  **Navigate to the Experiment Directory:**

    * Change your current directory to the specific experiment you want to run (e.g., `experiments/experiment_1`).

    ```bash
    cd experiments/experiment_1
    ```

3.  **Execute the Experiment Script:**

    * Use the `python -m` command to run the script located in the `bin` directory. This is essential for relative imports to work correctly.

    ```bash
    python -m bin.run
    ```

    * The `-m` flag tells Python to run `bin.run` as a module, treating `bin` as a package.

### Important Notes

* **Relative Imports:**
    * The scripts within the `bin` directory use relative imports (e.g., `from .src.module import function`). These imports rely on the `bin` directory being treated as a package, which is why `__init__.py` is required.
* **Virtual Environments:**
    * Always activate your virtual environment before running experiments. This ensures that you're using the correct project dependencies.
* **Isolation:**
    * Each experiment directory is designed to be isolated. The `src` directories contain the specific code for each experiment, and relative imports ensure that the code within an experiment only accesses its own `src` directory.
* **`python -m`:**
    * This command is critical for executing scripts within packages and ensuring that relative imports work as expected.
```

