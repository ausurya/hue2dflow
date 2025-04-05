import logging
import os
import time
import yaml
import requests
import json

# --- Configuration ---
LOG_FILE = 'conversion.log'
NUM_FEEDBACK_ATTEMPTS = 3 # Max attempts using validation feedback
MAX_RETRIES = 2 # Max retries for YAML generation if validation fails
RETRY_DELAY_SECONDS = 5 # Delay between retries
DEFAULT_CLUSTER_SETTINGS = { # As per FR2.3
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2
}

# --- Databricks Model Serving Configuration ---
# These should be set as environment variables or configured in Databricks
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://your-databricks-instance.cloud.databricks.com")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
MODEL_ENDPOINT_NAME = os.environ.get("MODEL_ENDPOINT_NAME", "claude-model-endpoint")

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'), # Overwrite log file each run
        logging.StreamHandler() # Also print logs to console
    ]
)

# --- LLM Interaction Function ---
def call_llm(prompt: str, context: str, max_tokens: int = 2000, model: str = "claude-3-7-sonnet-20250219") -> str:
    """Calls the Databricks model serving endpoint with the given prompt and context."""
    full_prompt = f"{prompt}\n\n{context}"
    logging.info(f"--- Sending Prompt to Databricks Model Endpoint: {MODEL_ENDPOINT_NAME} ---")
    logging.debug(f"Prompt Instruction: {prompt}")
    logging.debug(f"Context (first 200 chars): {context[:200]}...")

    try:
        # Prepare the request for Databricks model serving
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # Format the request based on how your model endpoint is configured
        # This is a typical format for Claude models deployed on Databricks
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": full_prompt
                }
            ],
            "max_tokens": max_tokens
        }
        
        # Construct the endpoint URL
        endpoint_url = f"{DATABRICKS_HOST}/serving-endpoints/{MODEL_ENDPOINT_NAME}/invocations"
        
        # Make the API call
        start_time = time.time()
        response = requests.post(endpoint_url, headers=headers, json=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Parse the response
        result = response.json()
        
        # Extract the content from the response
        # The exact structure depends on how your model endpoint returns data
        # Adjust this based on your specific model endpoint's response format
        if "content" in result:
            content = result["content"]
        elif "choices" in result and len(result["choices"]) > 0:
            content = result["choices"][0]["message"]["content"]
        else:
            content = str(result)  # Fallback if structure is different
            
        # Log token usage if available
        token_count = result.get("usage", {}).get("completion_tokens", 0)
        if token_count:
            logging.info(f"--- Received Model Response (Tokens: {token_count}) ---")
        else:
            logging.info(f"--- Received Model Response ---")
            
        # Calculate and log response time
        response_time = time.time() - start_time
        logging.debug(f"Response time: {response_time:.2f} seconds")
        
        return content
        
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response status: {e.response.status_code}")
            logging.error(f"Response body: {e.response.text}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during LLM call: {e}")
        raise

# --- Core Functions ---
def read_xml_file(file_path: str) -> str:
    """Reads the content of the XML file."""
    try:
        with open(file_path, 'r') as f:
            xml_content = f.read()
        return xml_content
    except FileNotFoundError:
        logging.error(f"XML file not found at {file_path}")
        raise
    except Exception as e:
        logging.error(f"Error reading XML file: {e}")
        raise

def generate_summary(xml_content: str) -> str:
    """Step 1: Generate summary from XML using LLM."""
    prompt = """Summarize this Oozie XML workflow in plain English. 
    Focus ONLY on:
    1. The workflow name
    2. All task names (actions, decision nodes, fork/join nodes)
    3. The dependencies between tasks (which task leads to which)
    
    Be comprehensive but concise."""
    summary = call_llm(prompt, xml_content)
    logging.info("Generated workflow summary.")
    return summary

def map_tasks(summary: str) -> str:
    """Step 2: Map tasks from summary using LLM."""
    prompt = """From the workflow summary, extract a structured list of ALL elements (tasks, decisions, forks, joins).
    For each element, provide ONLY:
    
    1. Element Name: The exact identifier
    2. Element Type: (action, decision, fork, join, kill, end)
    3. Transitions: Which tasks this element connects to (both success and error paths)
    
    Format as a numbered list with clear headings for each element."""
    task_list = call_llm(prompt, summary)
    logging.info("Mapped tasks from summary.")
    return task_list

def build_yaml(task_list: str) -> str:
    """Step 3: Build Databricks YAML from task list using LLM with retries."""
    cluster_info = f"Use a job cluster named 'default_cluster' with these settings: Spark Version '{DEFAULT_CLUSTER_SETTINGS['spark_version']}', Node Type '{DEFAULT_CLUSTER_SETTINGS['node_type_id']}', {DEFAULT_CLUSTER_SETTINGS['num_workers']} workers."
    prompt = f"""Generate a Databricks Workflow YAML definition based on the provided task list.

IMPORTANT FORMATTING INSTRUCTIONS:
1. Output ONLY raw YAML with NO markdown formatting (no ```yaml tags)
2. Start with 'name:' as the first line
3. Use proper YAML indentation with 2 spaces

WORKFLOW STRUCTURE REQUIREMENTS:
1. Include a top-level 'name' key that EXACTLY matches the original workflow name
2. Create a 'tasks' list with all workflow elements
3. For EVERY task, regardless of original type, use this structure:
   - task_key: The EXACT element name from the input
   - description: Brief description of what the task does
   - notebook_task:
       - notebook_path: "/Workflows/{{task_key}}"
       - base_parameters: {{}} (empty parameters object)
   - job_cluster_key: 'default_cluster'

DEPENDENCIES REQUIREMENTS:
1. For standard tasks:
   - Use 'depends_on' with 'task_key' to establish the EXACT same workflow dependencies as the original
   - If a task has multiple upstream dependencies, include ALL of them
   
2. For decision nodes:
   - Implement as a notebook_task like all other tasks
   - Use 'depends_on' to establish the correct upstream dependencies
   - Do NOT try to implement the decision logic - just preserve the task name and dependencies

3. For fork/join patterns:
   - Implement fork and join nodes as notebook_tasks
   - Preserve the exact dependencies between tasks

{cluster_info}

Remember: Output ONLY the raw YAML with no markdown formatting, starting with 'name:'."""
    
    for attempt in range(MAX_RETRIES + 1):
        logging.info(f"Attempting to generate YAML (Attempt {attempt + 1}/{MAX_RETRIES + 1})")
        yaml_output = call_llm(prompt, task_list, max_tokens=3000) # Increase max_tokens for potentially long YAML

        # Clean the output to remove any markdown formatting
        if yaml_output.startswith("```yaml"):
            yaml_output = yaml_output.replace("```yaml", "", 1).strip()
        if yaml_output.endswith("```"):
            yaml_output = yaml_output[:-3].strip()
            
        # FR4.1: Validate YAML
        try:
            yaml.safe_load(yaml_output) # Try parsing the YAML
            if not yaml_output or not yaml_output.strip().startswith("name:"):
                raise yaml.YAMLError("Generated content does not start with 'name:'")
            logging.info("Generated YAML is valid.")
            return yaml_output # Return valid YAML
        except yaml.YAMLError as e:
            logging.warning(f"Generated YAML is invalid (Attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES:
                logging.info(f"Retrying YAML generation in {RETRY_DELAY_SECONDS} seconds...")
                # Provide feedback to the LLM on the next attempt:
                prompt += f"\n\nPrevious attempt failed validation with error: {e}. Please ensure you output ONLY raw YAML with no markdown formatting, starting with 'name:'."
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logging.error("Max retries reached for YAML generation. Skipping validation and saving.")
                # Clean up one last time before returning
                if yaml_output.startswith("```yaml"):
                    yaml_output = yaml_output.replace("```yaml", "", 1).strip()
                if yaml_output.endswith("```"):
                    yaml_output = yaml_output[:-3].strip()
                return yaml_output # Return the last attempt even if invalid after retries
        except Exception as llm_err:
             logging.error(f"LLM call failed during YAML generation: {llm_err}")
             if attempt < MAX_RETRIES:
                 time.sleep(RETRY_DELAY_SECONDS)
             else:
                 raise # Re-raise if max retries reached

    logging.error("Failed to generate valid YAML after multiple retries.")
    return "" # Return empty string if all retries fail

def validate_yaml_against_summary(yaml_output: str, summary: str) -> str:
    """Step 4: Validate YAML against summary using LLM."""
    prompt = """Review the Databricks Workflow YAML against the original workflow summary.
Focus ONLY on verifying:

1. WORKFLOW NAME:
   - Does the YAML workflow name exactly match the original?

2. TASK NAMES:
   - Are ALL tasks from the original workflow present with their exact names?
   - Are there any missing or extra tasks?

3. DEPENDENCIES:
   - Do the task dependencies accurately reflect the original workflow connections?
   - Check each task's depends_on section against the original flow.

4. NOTEBOOK TASK TYPE:
   - Confirm that ALL tasks use notebook_task type.

Provide specific feedback on any discrepancies in these four areas only."""
    
    validation_feedback = call_llm(prompt, f"YAML:\n```yaml\n{yaml_output}\n```\n\nSummary:\n{summary}")
    logging.info("Generated validation feedback.")
    return validation_feedback

def save_output(content: str, output_path: str) -> None:
    """Saves the generated content (YAML or logs) to a file."""
    try:
        with open(output_path, 'w') as f:
            f.write(content)
        logging.info(f"Successfully saved output to {output_path}")
    except Exception as e:
        logging.error(f"Error saving output to {output_path}: {e}")
        raise

def convert_oozie_to_databricks(input_xml_path, output_yaml_path=None):
    """
    Convert an Oozie XML workflow to Databricks YAML workflow.
    
    Parameters:
    -----------
    input_xml_path : str
        Path to the input Oozie XML file
    output_yaml_path : str, optional
        Path to save the output Databricks YAML file. If None, will use the same name as input with .yml extension
    
    Returns:
    --------
    tuple
        (yaml_content, validation_feedback) - The generated YAML content and validation feedback
    """
    # Determine output file path if not provided
    if output_yaml_path is None:
        input_dir = os.path.dirname(input_xml_path)
        base_name = os.path.splitext(os.path.basename(input_xml_path))[0]
        output_yaml_path = os.path.join(input_dir, f"{base_name}.yml")
    
    # Validation log path
    validation_log_path = os.path.splitext(output_yaml_path)[0] + "_validation.log"
    
    logging.info(f"Starting conversion for: {input_xml_path}")
    logging.info(f"Output YAML will be saved to: {output_yaml_path}")
    logging.info(f"Logs will be saved to: {LOG_FILE}")

    final_yaml = ""
    validation_feedback = "Validation skipped due to errors in previous steps."

    try:
        # Read XML
        xml_content = read_xml_file(input_xml_path)
        logging.info(f"Successfully read XML file: {input_xml_path}")

        # Step 1 - Summarize
        summary = generate_summary(xml_content)

        # Step 2 - Map Tasks
        task_list = map_tasks(summary)

        # Step 3 - Build YAML (with retries)
        yaml_output = ""
        validation_feedback = "Initial run."
        last_yaml_output = ""

        for attempt in range(NUM_FEEDBACK_ATTEMPTS):
            logging.info(f"--- Starting Generation/Validation Attempt {attempt + 1}/{NUM_FEEDBACK_ATTEMPTS} ---")

            # Construct prompt for build_yaml, including feedback from previous attempts
            build_prompt_input = task_list
            if attempt > 0:
                logging.info("Incorporating previous validation feedback into the generation prompt.")
                build_prompt_input = f"Original Task List:\n{task_list}\n\nPrevious YAML Attempt:\n```yaml\n{last_yaml_output}\n```\n\nValidation Feedback on Previous Attempt:\n{validation_feedback}\n\nPlease revise the YAML based on the feedback to ensure accuracy according to the original task list and summary."

            # Build YAML
            yaml_output = build_yaml(build_prompt_input) # build_yaml has its own internal retries for basic validity
            if not yaml_output:
                logging.error(f"build_yaml failed to produce output on attempt {attempt + 1}.")
                continue # Try next feedback loop iteration if possible

            logging.info(f"Step 3 Generated YAML (Attempt {attempt + 1}):\n{yaml_output}")
            last_yaml_output = yaml_output # Store for potential next iteration

            # Validate YAML
            validation_feedback = validate_yaml_against_summary(yaml_output, summary)
            logging.info(f"Step 4 Validation Feedback (Attempt {attempt + 1}):\n{validation_feedback}")

            # Check if feedback indicates the YAML is good enough to stop early
            positive_indicators = [
                "accurately represent", "correctly captures", "matches the original", 
                "good representation", "properly implemented", "correctly implemented",
                "well structured", "appropriate", "accurate", "complete"
            ]
            negative_indicators = [
                "discrepancies", "missing", "incorrect", "issue", "problem", 
                "error", "not accurate", "fails to", "doesn't match", "inconsistent"
            ]
            
            # Count positive and negative indicators
            positive_count = sum(1 for indicator in positive_indicators if indicator.lower() in validation_feedback.lower())
            negative_count = sum(1 for indicator in negative_indicators if indicator.lower() in validation_feedback.lower())
            
            # Calculate a score based on the indicators
            feedback_score = positive_count - (negative_count * 1.5)
            
            # Log the analysis of the feedback
            logging.info(f"Feedback analysis - Positive indicators: {positive_count}, Negative indicators: {negative_count}, Score: {feedback_score}")
            
            # If the score is positive or we find explicit "no issues" statements, stop early
            if feedback_score > 0 or any(f"no {neg}" in validation_feedback.lower() for neg in negative_indicators):
                logging.info(f"Validation feedback seems positive on attempt {attempt + 1}. Stopping feedback loop early.")
                break

        # Save Outputs
        logging.info(f"--- Feedback loop finished after {attempt + 1} attempts. Saving final results. ---")
        save_output(last_yaml_output, output_yaml_path)
        logging.info(f"Successfully saved final YAML output to {output_yaml_path}")

        save_output(validation_feedback, validation_log_path)
        logging.info(f"Validation feedback saved to {validation_log_path}")
        
        return last_yaml_output, validation_feedback

    except Exception as e:
        logging.error(f"An critical error occurred during the conversion process: {e}", exc_info=True)
        return None, f"Conversion failed: {str(e)}"
    finally:
        logging.info("Conversion process completed.")

# For Databricks notebook usage
def main():
    """
    Example usage in a Databricks notebook:
    
    ```python
    # Set these variables in your notebook
    input_xml_path = "/dbfs/path/to/your/workflow.xml"
    output_yaml_path = "/dbfs/path/to/your/output.yml"
    
    # Run the conversion
    yaml_content, validation_feedback = convert_oozie_to_databricks(input_xml_path, output_yaml_path)
    
    # Display results
    print(f"Conversion completed. Output saved to: {output_yaml_path}")
    print("\nValidation Feedback:")
    print(validation_feedback)
    ```
    """
    # This is just a placeholder for when the script is run directly
    # In Databricks, you would call convert_oozie_to_databricks() directly
    print("This script is designed to be imported and used in a Databricks notebook.")
    print("Example usage:")
    print("  from generate_dbr import convert_oozie_to_databricks")
    print("  yaml_content, validation_feedback = convert_oozie_to_databricks(input_xml_path, output_yaml_path)")

if __name__ == "__main__":
    main()
