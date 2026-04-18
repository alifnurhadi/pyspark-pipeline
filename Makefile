.PHONY: help install run clean format

ifeq ($(OS), Windows_NT)
    PYTHON      := python
    RM_RF       := rmdir /s /q
    FIND_CACHE  := for /d /r . %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"
    SET_PATH    := set PYTHONPATH=. &&
else
    PYTHON      := python3
    RM_RF       := rm -rf
    FIND_CACHE  := find . -type d -name "__pycache__" -exec rm -rf {} +
    SET_PATH    := PYTHONPATH=.
endif

help:
	@echo "Available commands:"
	@echo "  make install  - Install dependencies using uv"
	@echo "  make run      - Run the PySpark data pipeline"
	@echo "  make clean    - Clean up PySpark metadata, logs, and output directories"
	@echo "  make format   - Format code using standard python tools"

install:
	@echo "Installing dependencies via uv..."
	uv pip install -r pyproject.toml

run:
	@echo "Executing the data pipeline orchestrator..."
	$(SET_PATH) $(PYTHON) job/pipeline.py --config config/pipeline.yaml

clean:
	@echo "Cleaning up project artifacts..."
	-$(RM_RF) output/
	-$(RM_RF) data/quarentine/
	$(FIND_CACHE)
	@echo "Clean complete."

format:
	@echo "Formatting code..."
	ruff format . || black .
