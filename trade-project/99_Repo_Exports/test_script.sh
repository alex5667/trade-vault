#!/bin/sh
sh -c "echo 'hello'; \
  wrong_cmd \
    --arg1 \
    --arg2"
