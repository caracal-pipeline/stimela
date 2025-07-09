#!/bin/bash

# process directory bind directives of the form
# /tmp::NAME

declare -A fsdict   # dict name -> directory
names=()            # list of names

while [ "$1" != "--" -a "$1" != "" ]; do
  fs="${1%%::*}"
  name="${1##*::}"
  shift
  if [ "$fs" == "" -o "$name" == "" ]; then
    echo "Invalid tmpfs spec '$1'"
    exit 1
  fi
  if [ ! -w "$fs" ]; then 
    echo "Invalid tmpfs spec '$1:' $fs is no writable"
    exit 1
  fi
  names+=("$name")
  fsdict[$name]=`mktemp -d -p $fs`
  echo "Created tmpfs ${fsdict[$name]} for $name"
done

# now we expect a command and arguments
if [ "$1" == "--" ]; then
  shift
fi

if [ "$1" == "" ]; then
  echo "Usage: slurm_runner.sh DIR::NAME [...] -- command [args]"
  exit 1
else
  command="$1"
  shift
fi

args=()
# now remap arguments
for arg in "$@"; do
  a="$arg"
  for name in "${!fsdict[@]}"; do
    a="${a//::$name::/${fsdict[$name]}}"
  done
  args+=("$a")
done

## uncomment for debugging
# echo "names: ${names[@]}"
# echo "fsdict: ${!fsdict[@]}"
# echo "command: $command"
# for arg in "${args[@]}"; do
#   echo "argument: $arg"
# done

clean_fs() {
  for fs in "${fsdict[@]}"; do
    if [ -f "$fs" ]; then
      echo "Removing $fs"
      rm -fr $fs
    fi
  done
  ## uncomment for debugging
  # echo "Tmpfs cleanup complete"
}

# Cleanup function to be called on script exit or interruption
cleanup() {
    echo "Caught interrupt. Cleaning up..."
    if [[ -n "$JOB_PID" ]]; then
        echo "Killing job with PID $JOB_PID"
        kill -INT "$JOB_PID" 2>/dev/null
        wait "$JOB_PID"
    fi
    clean_fs
    exit 1
}

# Trap SIGINT (Ctrl+C) and call cleanup
trap cleanup SIGINT

# Run the job in the background
$command "${args[@]}" &
JOB_PID=$!

# Wait for the job to finish
wait $JOB_PID
EXIT_CODE=$?

echo "Job finished with exit code $EXIT_CODE"
# Final cleanup
clean_fs

exit $EXIT_CODE
