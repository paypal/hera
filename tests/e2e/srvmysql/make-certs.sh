# https://pki-tutorial.readthedocs.io/en/latest/simple/
mkdir temp
cd temp

mkdir -p demoCA/newcerts
mkdir -p demoCA/private
if [ ! -f demoCA/serial ]
then
	printf 1f%04x $RANDOM > demoCA/serial
fi
touch demoCA/index.txt

cat << EOF > ca.cfg
[ req ]
prompt = no
distinguished_name = ca_dn

[ ca_dn ]
organizationName = "OCC Test"
commonName = "cert-authority"
countryName = "US"
stateOrProvinceName = "California"
EOF

openssl req -new -config ca.cfg -out ca.csr -keyout demoCA/private/cakey.pem -nodes -newkey rsa:2048

echo Making self-signed CA
yes | openssl ca -selfsign -in ca.csr -out demoCA/cacert.pem -keyfile demoCA/private/cakey.pem -days 4000


cat << EOF > srv.cfg
[ req ]
prompt = no
distinguished_name = ca_dn

[ ca_dn ]
organizationName = "OCC Test"
commonName = "occ test"
countryName = "US"
stateOrProvinceName = "California"
EOF
openssl req -new -config srv.cfg -out srv.csr -keyout srv.key -nodes -newkey rsa:2048

echo CA Signing
yes | openssl ca -in srv.csr -out srv.crt -days 3000

# make an encrypted private key
openssl rsa -in srv.key -out srv2.key -des -passout pass:35-Out
cp srv2.key ..

keytool -importcert -keystore ../srv.jks -file demoCA/cacert.pem -alias testCa -storepass 61-Moog -noprompt

cat srv.crt demoCA/cacert.pem > ../srvChain.crt
cp srv.key ..
