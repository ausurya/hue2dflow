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

## Directory Structure

* `examples/` - Contains example Oozie workflow XML files and their corresponding YAML outputs
  * `sample-workflow.xml` - Simple workflow with basic actions
  * `sample-workflow.yml` - Generated Databricks workflow from sample XML
  * `complex-workflow.xml` - Workflow with decision nodes and fork/join patterns
  * `complex-workflow.yml` - Generated Databricks workflow from complex XML
  * `advanced-workflow.xml` - Complex workflow with multiple decision nodes, fork/join patterns, and diverse action types
  * `advanced-workflow.yml` - Generated Databricks workflow from advanced XML

* `generate.py` - Main conversion script using Anthropic API directly
* `generate_dbr.py` - Conversion script using Databricks model serving endpoints

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

4.  **Set API Keys:**
    
    For `generate.py` (using Anthropic API directly):
    ```bash
    export ANTHROPIC_API_KEY='your_api_key_here'
    ```
    
    For `generate_dbr.py` (using Databricks model serving):
    ```bash
    export DATABRICKS_HOST='https://your-databricks-instance.cloud.databricks.com'
    export DATABRICKS_TOKEN='your-databricks-token'
    export MODEL_ENDPOINT_NAME='your-claude-model-endpoint-name'
    ```
    
    *⚠️ **Security Note:** Do not hardcode your API keys directly in the script. Use environment variables or a secure secrets management solution.*

## Usage

### Using Anthropic API directly:

```bash
# Convert a workflow XML file
python generate.py examples/sample-workflow.xml

# Specify output file path
python generate.py examples/sample-workflow.xml -o custom_output.yml
```

### Using Databricks model serving:

```bash
# Convert a workflow XML file
python generate_dbr.py examples/sample-workflow.xml

# Specify output file path
python generate_dbr.py examples/sample-workflow.xml -o custom_output.yml
```

## Output

The script generates:
1. A YAML file with the Databricks workflow definition (in the same directory as the input XML file)
2. A validation log file with feedback on the conversion (in the same directory as the input XML file)
3. A detailed conversion log file (`conversion.log`) in the current directory

For example, if you run:
```bash
python generate.py examples/sample-workflow.xml
```

The following files will be generated:
- `examples/sample-workflow.yml` - The Databricks workflow YAML
- `examples/sample-workflow_validation.log` - Validation feedback
- `conversion.log` - Detailed conversion logs
