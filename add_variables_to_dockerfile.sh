openssl enc -d -aes-256-cbc -pbkdf2 -iter 20000 -in .env.shared.enc -out .env.shared -k $ENV_SECRET

# Mask values in Github actions output
./mask_values.sh

# Prepends the needed `ENV` docker command
sed -i -e 's/^./ENV &/' .env.shared

#Put the variables into the Dockerfile.
cat .env.shared >> Dockerfile