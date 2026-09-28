# src/rhosocial/activerecord/backend/impl/oracle/backend/__init__.py
"""Oracle backend implementations.

Every backend keeps both classes in this package: the sync class in
``backend.py`` and the async class in ``async_backend.py``. So the sync class
is at ``impl.oracle.backend.backend`` and the async class at
``impl.oracle.backend.async_backend``.
"""

from .backend import _is_numeric_python_type, OracleBackend
