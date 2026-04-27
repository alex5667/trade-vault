import psycopg2
import sys

# use a simple query
# psycopg2 execute doesn't need to actually connect, we can just use string interpolation to see the exception
try:
    from psycopg2.extensions import adapt
    pass
except Exception:
    pass

try:
    print( "asd %s asd %s" % (1,) )
except Exception as e:
    print(type(e).__name__, e)
