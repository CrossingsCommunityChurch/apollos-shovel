npx @apollosproject/npx @apollosproject/apollos-cli secrets -d $ENV_SECRET

# Prepends the needed `ENV` docker command
sed -i -e 's/^./ENV &/' .env.shared

#Put the variables into the Dockerfile.
cat .env.shared >> Dockerfile