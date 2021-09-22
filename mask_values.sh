# https://docs.github.com/en/actions/reference/workflow-commands-for-github-actions#masking-a-value-in-log
grep . .env.shared | xargs -I % sh -c 'echo "::add-mask::$(echo % | sed s/^[A-Z_]*=//g)"'