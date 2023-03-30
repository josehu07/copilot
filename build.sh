#!/usr/bin/bash

COPILOT_DIR="$HOME/copilot"
cd $COPILOT_DIR

echo "Building golang binaries..."
make
if [[ $? -ne 0 ]]; then
    echo "Error: make failed"
    exit 1
fi

echo "Prefixing binary names with 'copilot-'..."
cd bin
rm -f $(ls . | grep copilot-)
rename 's/^(.*)$/copilot-$1/' $(ls .)
if [[ $? -ne 0 ]]; then
    echo "Error: renaming failed"
    exit 1
fi
