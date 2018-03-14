#!/bin/bash

# Build zip
zip build environment.yml pipelinefinisher/* Dockerfile

# Deploy to EB
eb deploy
