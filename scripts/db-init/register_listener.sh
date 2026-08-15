#!/bin/bash
# Register Oracle services with the TCP listener.
# This resolves the issue where PMON does not auto-register services
# after container startup in gvenzl/oracle-xe images (18c/21c).
set -Eeuo pipefail

sqlplus -s / as sysdba <<EOF
   WHENEVER SQLERROR EXIT SQL.SQLCODE
   ALTER SYSTEM SET LOCAL_LISTENER='(ADDRESS=(PROTOCOL=TCP)(HOST=0.0.0.0)(PORT=1521))' SCOPE=BOTH;
   ALTER SYSTEM REGISTER;
   EXIT;
EOF