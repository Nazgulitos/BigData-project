#!/bin/bash
echo "Starting Stage 3: Flight cancellation prediction pipeline"
spark-submit --master yarn scripts/ml_pipeline.py
echo "Stage 3 automation script finished."
