import os
import pandas as pd
import tiktoken
from openai import AzureOpenAI, BadRequestError
from tenacity import retry, stop_after_attempt, wait_exponential


##################################################
#    API Configuration & access
##################################################

"""
Assumes authentication info is stored in local environment variables.

If not, you can find the API key, API version, and endpoint URL by following
these steps:
    1. Log into the Azure portal and view all resources.
    2. Navigate to the 'Azure AI project' resource page named 'me-4539'.
    3. Launch the AI Studio (there should be a button somewhere).
    4. Go to Components > Deployments.
    5. Choose an existing model that suits your needs or create a new model deployment.
    6. Click on the model details to find API key and other info.
    7. Save the info you need to an environment variable. Don't hardcode the API keys or endpoint.

* Notes: 
    - All models under the same project resource use the same API key and endpoint. To change
    the model, just change the model name.
    - Try not to confuse 
"""

class AzureAPIConfig:
    def __init__(self, api_key: str, api_version: str, endpoint: str) -> None:
        self.api_key = api_key
        self.api_version = api_version
        self.endpoint = endpoint

def get_default_api_config(choice=0):
    configurations = [
        AzureAPIConfig(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"), 
            api_version="2024-02-15-preview", 
            endpoint=os.getenv("AZURE_OPENAI_LANGUAGE_ENDPOINT")
            )
        ]
    return configurations[choice]


##################################################
#    Configurations for Azure OpenAI Models
##################################################

class AzureOpenAIConfig:
    def __init__(self, max_tokens: int, max_output: int, tokens_per_minute_limit: int, requests_per_minute_limit: int, 
                 deployment: str, model: str, max_iterations: int, input_cost: float, output_cost: float, 
                 max_input: int = None, max_array_size: int = None):
        self.max_tokens = max_tokens
        self.max_output = max_output
        self.max_input = max_input if max_input is not None else max_tokens - max_output
        self.tokens_per_minute_limit = tokens_per_minute_limit
        self.requests_per_minute_limit = requests_per_minute_limit
        self.deployment = deployment
        self.model = model
        self.max_iterations = max_iterations
        self.input_cost = input_cost
        self.output_cost = output_cost
        self.max_array_size = max_array_size
        

def get_default_model_config(choice=0):
    configurations = [
        AzureOpenAIConfig(
            max_tokens=128000,
            max_output=4096,
            tokens_per_minute_limit=70000,
            requests_per_minute_limit=420,
            deployment="gpt-4-TPM-70k-RPM-420",
            model="gpt-4-turbo",
            max_iterations=100,
            input_cost=0.01,
            output_cost=0.03
        ),
        AzureOpenAIConfig(
            max_tokens=128000,
            max_output=4096,
            tokens_per_minute_limit=10000,
            requests_per_minute_limit=60,
            deployment="gpt-4-turbo",
            model="gpt-4-turbo",
            max_iterations=100,
            input_cost=0.01,
            output_cost=0.03
        ),
        AzureOpenAIConfig(
            max_tokens=128000,
            max_output=4096,
            tokens_per_minute_limit=50000,
            requests_per_minute_limit=300,
            model="text-embedding-ada-002",
            deployment="text-embedding-ada-002",
            max_iterations=100,
            input_cost=0.01,
            output_cost=0.03,
            max_array_size=2048
            )
        ]
    return configurations[choice]


##################################################
#    Helper functions
##################################################

def get_tokenizer(model_config: AzureOpenAIConfig):
    try:
        encoding = tiktoken.encoding_for_model(model_config.model)
    except KeyError:
        print("Warning: Encoding not found. Using cl100k_base encoding.")
        encoding = tiktoken.get_encoding("cl100k_base")
    return encoding


def estimate_num_tokens_from_str(string, model_config):
    """
    Returns the number of tokens in a text string.
    """
    encoding = get_tokenizer(model_config)
    return len(encoding.encode(string))


def get_cost(usage_obj, model_config: AzureOpenAIConfig):
    """
    Returns the total cost of an API call.
    """
    return ((usage_obj.prompt_tokens/1000) * model_config.input_cost) + ((usage_obj.completion_tokens/1000) * model_config.output_cost)


def chop_input(text: str, tokens_used: int, model_config: AzureOpenAIConfig):
    """
    Truncated the text if it exceeds max_tokens.
    """
    enc = get_tokenizer(model_config.model)
    available_tokens = model_config.max_tokens - tokens_used
    text = enc.decode(enc.encode(text)[:available_tokens])
    return text


def validate_total_tokens(df: pd.DataFrame, source_col: str, prompt: str, model_config: AzureOpenAIConfig):
    """
    Validates that no item in the dataset exceeds the token limit.
    """
    limit = min(model_config.tokens_per_minute_limit, model_config.max_input)
    prompt_batch_all = [prompt + item for item in df[source_col]]  # Append prompt to all items in the column
    for i, item in enumerate(prompt_batch_all):
        total_tokens = estimate_num_tokens_from_str(item, model_config)
        if total_tokens > limit:
            raise ValueError(f"Total token limit exceeded for item {i} in the dataset: {total_tokens} tokens, which is greater than the per-minute limit of {limit} tokens.")
    print(f"All items in dataset passed validation.")


def validate_total_tokens_batch(df: pd.DataFrame, source_col: str, prompt: str, model_config: AzureOpenAIConfig, batch_size: int):
    """
    Validates that the total number of tokens for the entire dataset does not exceed the set limit.
    """
    limit = min(model_config.tokens_per_minute_limit, model_config.max_input)
    prompt_batch_all = [prompt + item for item in df[source_col]]  # Append prompt to all items in the column

    for i in range(0, len(df), batch_size):
        data_batch = prompt_batch_all[i:i+batch_size]
        joined_batch = ' '.join(data_batch)
        total_tokens = estimate_num_tokens_from_str(joined_batch, model_config)
        if total_tokens > limit:
            raise ValueError(f"Total token limit exceeded for the batch: {total_tokens} tokens, which is greater than the per-minute limit of {limit} tokens.")
        print(f"Total tokens in batch: {total_tokens} are within the allowed limit of {limit} tokens.")
    print('All batches passed validation.')



##################################################
#    Requests functions w/ Exponential Backoff
##################################################

@retry(wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
def get_completion_json(prompt: str, model_config: AzureOpenAIConfig, api_config: AzureAPIConfig):
    client = AzureOpenAI(
        api_key=api_config.api_key,  
        api_version=api_config.api_version,
        azure_endpoint=api_config.endpoint
    )
    try:
        response = client.chat.completions.create(
                model=model_config.deployment,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=model_config.max_output,
                stop=None,
                n=1,
                response_format= {
                                "type": "json_object"
                                }
            )
        return response
    except BadRequestError as e:  # Specific handling for HTTP 400 error (ResponsibleAIPolicyViolation)
        if e.status_code == 400:  # Check if the error is a 400
            print(f"Error code 400 encountered: Bad Request - {e}")
            return False
        else:
            print(f"An error occurred: {e}")
            raise  # Reraise other HTTP errors
    except Exception as e:
        print(f"An error occurred: {e}")
        raise


@retry(wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(15))
def get_completion_string(prompt: str, model_config: AzureOpenAIConfig, api_config: AzureAPIConfig):
    client = AzureOpenAI(
        api_key=api_config.api_key,  
        api_version=api_config.api_version,
        azure_endpoint=api_config.endpoint
    )
    try:
        response = client.chat.completions.create(
                model=model_config.deployment,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=model_config.max_output,
                stop=None,
                n=1
            )
        return response
    except BadRequestError as e:  # Specific handling for HTTP 400 error (ResponsibleAIPolicyViolation)
        if e.status_code == 400:  # Check if the error is a 400
            print(f"Error code 400 encountered: Bad Request - {e}")
            return False
        else:
            print(f"An error occurred: {e}")
            raise  # Reraise other HTTP errors
    except Exception as e:
        print(f"An error occurred: {e}")
        raise


# @retry(wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
# def get_completion_string_batch(prompt_batch, model_config: AzureOpenAIConfig, api_config: AzureAPIConfig):
#     client = AzureOpenAI(
#         api_key=api_config.api_key,  
#         api_version=api_config.api_version,
#         azure_endpoint=api_config.endpoint
#     )
#     try:
#         response = client.chat.completions.create(
#                 model=model_config.deployment,
#                 messages=[{"role": "user", "content": prompt_batch}],
#                 max_tokens=model_config.max_output,
#                 stop=None,
#                 n=1,
#                 temperature=0
#             )
#         return response.choices
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         raise


##################################################
#    Pandas functions
##################################################

def add_filter_column(df: pd.DataFrame, source_col: str, target_col: str, prompt: str, 
                      model_config: AzureOpenAIConfig, api_config: AzureAPIConfig):
    """ Use GPT to extract features from a column in a dataframe.

    Params:
    df: Dataframe to add column to
    source_col: Name of column containing source data
    target_col: Name of new column to be created
    prompt: Prompt to pass into completions API
    model_config: Azure openAI model configuration
    api_configuration: Azure openAI API configuration
    """
    validate_total_tokens(df, source_col, prompt, model_config)  # Perform token validation over the entire dataset

    total_items_processed = 0
    total_api_cost = 0.0
    result_df = df.copy(deep=True)
    result_df.loc[:, target_col] = ''  # Creating empty column to store result

    for i in range(0, len(df)):
        try:
            datapoint = df[source_col][i]
            current_prompt = prompt + datapoint

            response = get_completion_string(current_prompt, model_config, api_config)
            if response:
                result_df.loc[i, target_col] = response.choices[0].message.content 
                total_api_cost += get_cost(response.usage, model_config)
            else:
                result_df.loc[i, target_col] = response
                total_api_cost += (estimate_num_tokens_from_str(current_prompt, model_config)/1000 * model_config.input_cost)

            total_items_processed += 1

            print(f"Total processed so far: {total_items_processed}/{len(df)}, Cost so far: ${total_api_cost:.2f}")

        except Exception as e:
            print(f"Error processing item at index {i}: {e}")
            return result_df
    return result_df


# def add_json_filter_columns(df: pd.DataFrame, source_col: str, target_col: list[str], prompt: str, 
#                       model_config: AzureOpenAIConfig, api_config: AzureAPIConfig):
#     """ Use GPT to extract features from a column in a dataframe.

#     Params:
#     df: Dataframe to add column to
#     source_col: Name of column containing source data
#     target_col: Name of new column to be created
#     prompt: Prompt to pass into completions API
#     model_config: Azure openAI model configuration
#     api_configuration: Azure openAI API configuration
#     """
#     validate_total_tokens(df, source_col, prompt, model_config)  # Perform token validation over the entire dataset

#     total_items_processed = 0
#     total_api_cost = 0.0
#     result_df = df.copy(deep=True)
#     for col in target_col:
#         result_df.loc[:, col] = ''  # Creating empty columns to store results

#     for i in range(0, len(df)):
#         try:
#             datapoint = df[source_col][i]
#             current_prompt = prompt + datapoint

#             response = get_completion_json(current_prompt, model_config, api_config)
#             if response:
#                 response_json = response.choices[0].message.content 
#                 result_df.loc[i, target_col] = 
#                 total_api_cost += get_cost(response.usage, model_config)
#             else:
#                 result_df.loc[i, target_col] = response
#                 total_api_cost += (estimate_num_tokens_from_str(current_prompt, model_config)/1000 * model_config.input_cost)

#             total_items_processed += 1

#             print(f"Total processed so far: {total_items_processed}/{len(df)}, Cost so far: ${total_api_cost:.2f}")

#         except Exception as e:
#             print(f"Error processing item at index {i}: {e}")
#             return result_df
#     return result_df


# def add_filter_column_batch(df: pd.DataFrame, source_col: str, target_col: str, prompt: str, 
#                       model_config: AzureOpenAIConfig, api_config: AzureAPIConfig,
#                       batch_size=10):
#     """ Use GPT to extract features from a column in a dataframe.

#     Params:
#     df: Dataframe to add column to
#     source_col: Name of column containing source data
#     target_col: Name of new column to be created
#     prompt: Prompt to pass into completions API
#     model_config: Azure openAI model configuration
#     api_configuration: Azure openAI API configuration
#     batch_size: number of calls sent in parallel
#     """
#     validate_total_tokens_batch(df, source_col, prompt, model_config, batch_size)  # Perform token validation over the entire dataset

#     total_items_processed = 0
#     total_api_cost = 0.0
#     result_df = df[[source_col]]
#     result_df.loc[:, target_col] = ''  # Creating empty column to store result

#     for i in range(0, len(df), batch_size):
#         data_batch = df[source_col][i:i+batch_size]
#         prompt_batch = [prompt + item for item in data_batch]  # Send entire batch in same call

#         try:
#             batch_response = get_completion_string_batch(prompt_batch, model_config, api_config)

#             for response in batch_response.choices:
#                 result_df.loc[i + response.index, target_col] = response.message.content  # Match completions response by index

#             total_api_cost += get_cost(batch_response.usage, model_config)
#             total_items_processed += len(batch_response.choices)

#             print(f"Total processed so far: {total_items_processed}/{len(df)}, Cost so far: ${total_api_cost:.2f}")

#         except Exception as e:
#             print(f"Error processing batch starting at index {i}: {e}")
#     return result_df