1. Run vLLM  `vllm serve unsloth/gemma-3-270m-it-unsloth-bnb-4bit --port 8000 --swap-space 0 --gpu-memory-utilization 0.8 --max-model-len 4096`
2. Ingest `synthetic-data-kit -c config.yaml ingest data/input/`
3. Generate `python generate.py`
4. Curate `python curate.py`
5. Format `python format.py`