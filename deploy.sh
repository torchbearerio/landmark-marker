#!/bin/bash

# Build zip
zip build -r environment.yml pipelinefinisher Dockerfile .ebextensions

# Deploy to EB
eb deploy
