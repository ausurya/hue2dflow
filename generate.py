import argparse
import logging
import os
import time
import yaml
from anthropic import Anthropic, AnthropicError, RateLimitError

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

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'), # Overwrite log file each run
        logging.StreamHandler() # Also print logs to console
    ]
)

# --- LLM Client Initialization ---
try:
    # Explicitly get API key from environment
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logging.error("ANTHROPIC_API_KEY environment variable not set.")
        exit(1)
    
    # Initialize the client with the API key
    llm_client = Anthropic(api_key=api_key)
except ImportError:
    logging.error("Anthropic library not found. Please install it: pip install anthropic")
    exit(1)
except Exception as e:
    logging.error(f"Failed to initialize Anthropic client: {e}")
    logging.error("Ensure the ANTHROPIC_API_KEY environment variable is set correctly.")
    exit(1)

# --- LLM Interaction Function ---
def call_llm(prompt: str, context: str, max_tokens: int = 2000, model: str = "claude-3-7-sonnet-20250219") -> str:
    """Calls the Anthropic API with the given prompt and context."""
    full_prompt = f"\n\nHuman: {prompt}\n\n{context}\n\nAssistant:"
    logging.info(f"--- Sending Prompt to LLM (Model: {model}) ---")
    # Log only the instruction part of the prompt, not the full context
    logging.debug(f"Prompt Instruction: {prompt}")
    logging.debug(f"Context (first 200 chars): {context[:200]}...")

    try:
        response = llm_client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=[
                {
                    "role": "user",
                    "content": f"{prompt}\n\n{context}"
                }
            ]
        )

        # Extract text content from the response blocks
        result = "".join([block.text for block in response.content if hasattr(block, 'text')]).strip()

        logging.info(f"--- Received LLM Response (Tokens: {response.usage.output_tokens}) ---")
        logging.debug(f"LLM Response (first 200 chars): {result[:200]}...")
        if not result:
             logging.warning("LLM returned an empty response.")
        return result

    except RateLimitError as e:
        logging.error(f"Anthropic API rate limit exceeded: {e}. Please check your plan and usage.")
        raise
    except AnthropicError as e:
        logging.error(f"Anthropic API error: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during the LLM call: {e}")
        raise

# --- Core Functions ---
def read_xml_file(file_path: str) -> str:
    """Reads the content of the XML file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        logging.info(f"Successfully read XML file: {file_path}")
        return content
    except FileNotFoundError:
        logging.error(f"Error: Input XML file not found at {file_path}")
        raise
    except Exception as e:
        logging.error(f"Error reading XML file {file_path}: {e}")
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
    
    # Use a potentially cheaper/faster model for validation if desired - Using Sonnet now for consistency
    validation_feedback = call_llm(prompt, f"YAML:\n```yaml\n{yaml_output}\n```\n\nSummary:\n{summary}", model="claude-3-7-sonnet-20250219")
    logging.info("Generated validation feedback.")
    return validation_feedback

def save_output(content: str, output_path: str):
    """Saves the generated content (YAML or logs) to a file."""
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)
        logging.info(f"Successfully saved output to {output_path}")
    except Exception as e:
        logging.error(f"Error writing file to {output_path}: {e}")
        # Don't raise here, allow main process to finish if possible

# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Convert Oozie XML workflow to Databricks Workflow YAML using LLM chaining.")
    parser.add_argument("xml_file", help="Path to the input Oozie workflow XML file.")
    parser.add_argument("-o", "--output", help="Optional: Path to save the output YAML file.")
    args = parser.parse_args()

    input_xml_path = args.xml_file
    # Determine output path (FR3.1)
    if args.output:
        output_yaml_path = args.output
    else:
        output_yaml_path = os.path.splitext(input_xml_path)[0] + ".yml"

    logging.info(f"Starting conversion for: {input_xml_path}")
    logging.info(f"Output YAML will be saved to: {output_yaml_path}")
    logging.info(f"Logs will be saved to: {LOG_FILE}")

    final_yaml = ""
    validation_feedback = "Validation skipped due to errors in previous steps."
    validation_log_path = os.path.splitext(output_yaml_path)[0] + "_validation.log"

    try:
        # FR1.1, FR1.2: Read XML
        xml_content = read_xml_file(input_xml_path)

        # FR2.1: Step 1 - Summarize
        summary = generate_summary(xml_content)
        # logging.info(f"Step 1 Summary:\n{summary}") # Logged via debug in call_llm

        # FR2.2: Step 2 - Map Tasks
        task_list = map_tasks(summary)
        # logging.info(f"Step 2 Task List:\n{task_list}")

        # FR2.3: Step 3 - Build YAML (with retries)
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

        # --- Step 5: Save Outputs ---
        logging.info(f"--- Feedback loop finished after {attempt + 1} attempts. Saving final results. ---")
        save_output(last_yaml_output, output_yaml_path)
        logging.info(f"Successfully saved final YAML output to {output_yaml_path}")

        save_output(validation_feedback, validation_log_path)
        logging.info(f"Validation feedback saved to {validation_log_path}")

    except Exception as e:
        logging.error(f"An critical error occurred during the conversion process: {e}", exc_info=True)
        print(f"Conversion failed critically. Check {LOG_FILE} for details.")
        # Exit with a non-zero status code to indicate failure
        exit(1)
    finally:
        logging.info("Conversion process completed.")
        print(f"\nConversion finished.")
        print(f"Output YAML location: {output_yaml_path}")
        print(f"Validation feedback: {validation_log_path}")
        print(f"Detailed logs: {LOG_FILE}")

if __name__ == "__main__":
    main()
