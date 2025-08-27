from datasets import load_dataset

raw_datasets = load_dataset("code_search_net", "python", trust_remote_code=True)

print(raw_datasets["train"][123456]["whole_func_string"])