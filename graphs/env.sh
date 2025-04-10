#!/bin/bash

# mkdir -p ~/.local/share/fonts/otf
# cp -r LinLibertine ~/.local/share/fonts/otf/
# fc-cache
# rm -rf ~/.cache/matplotlib

mkdir -p plots
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt