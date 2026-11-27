#!/bin/sh
# The teaching stack publishes HTTP on Windows loopback only.
set -eu
. /opt/nifi/scripts/common.sh
prop_replace nifi.web.https.host ''
prop_replace nifi.web.https.port ''
prop_replace nifi.web.http.host 0.0.0.0
prop_replace nifi.web.http.port 8081
prop_replace nifi.remote.input.secure false
prop_replace nifi.security.keystore ''
prop_replace nifi.security.truststore ''
prop_replace nifi.security.user.login.identity.provider ''
prop_replace java.arg.2 -Xms512m "$nifi_bootstrap_file"
prop_replace java.arg.3 -Xmx1g "$nifi_bootstrap_file"
exec "$NIFI_HOME/bin/nifi.sh" run
