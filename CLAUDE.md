# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Open Patent is a mission to make patent data accessible to everyone. The project provides datasets collated from EPO publication servers and PatentWiew, plus synthetic datasets for machine learning training.

## Development Setup

This project uses Python 3.11+ with uv for dependency management.

Install dependencies:
```bash
uv sync
```

Activate the virtual environment:
```bash
source .venv/bin/activate
```

## Development rules

- Simplicity and readability are key. 
- Avoid complex code and unnecessary abstractions.
- Use type hints and docstrings to document code.
- Use descriptive variable names and comments to explain the code.
