# Oozie to Databricks Workflow Converter

This tool uses a multi-prompt LLM chain (Anthropic Claude) to convert Apache Oozie workflow XML files into Databricks Workflow YAML files.

## Features

*   Accepts an Oozie workflow XML file as input.
*   Uses a 4-step LLM chain:
    1.  **Summarize:** Understand the Oozie workflow.
    2.  **Map Tasks:** Extract structured task information.
    3.  **Build YAML:** Generate the Databricks Workflow YAML.
    4.  **Validate:** Compare the generated YAML against the initial summary.
*   Outputs a Databricks Workflow YAML file.
*   Includes basic YAML validation and retry logic.
*   Logs the conversion process and validation feedback.

## Setup

1.  **Clone the repository (or download the files):**
    ```bash
    # If using git
    # git clone <repository_url>
    # cd <repository_directory>
    ```

2.  **Set up a Python environment:**
    It's recommended to use a virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate # On Windows use `venv\Scripts\activate`
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Set Anthropic API Key:**
    The script requires access to the Anthropic Claude API. Set your API key as an environment variable:
    ```bash
    export ANTHROPIC_API_KEY='your_api_key_here'
    ```
    *⚠️ **Security Note:** Do not hardcode your API key directly in the script. Use environment variables or a secure secrets management solution.*

## Usage

Run the script from your terminal, providing the path to the Oozie XML file:

```bash
python generate.py path/to/your/workflow.xml
```

**Example:**

```bash
python generate.py sample-workflow.xml
```

**Options:**

*   `-o` or `--output`: Specify a custom path for the output YAML file. If omitted, the output file will be saved in the same directory as the input XML with a `.yml` extension (e.g., `workflow.xml` -> `workflow.yml`).

    ```bash
    python generate.py sample-workflow.xml -o output/databricks-workflow.yaml
    ```

## Output

*   **YAML File:** The generated Databricks Workflow YAML (e.g., `sample-workflow.yml`).
*   **Validation Log:** A text file containing the LLM's validation feedback (e.g., `sample-workflow_validation.log`).
*   **Conversion Log:** A detailed log of the conversion process (`conversion.log`).

## Limitations

*   **LLM Dependency:** The quality of the conversion heavily depends on the LLM's ability to understand the Oozie XML and generate accurate Databricks YAML. Complex Oozie features (sub-workflows, complex decision logic, forks/joins, Hadoop actions beyond basic Shell/Spark/Hive) might not be converted correctly without significant prompt engineering or additional parsing logic.
*   **No Deployment:** This tool only generates the YAML definition; it does not interact with the Databricks API to deploy the workflow.
*   **Error Handling:** While basic retry and validation are included, complex errors or highly malformed XML might cause failures.
*   **Default Cluster:** Uses hardcoded default cluster settings. Modify `generate.py` if different defaults are needed.
