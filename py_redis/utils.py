import logging
import time

logger = logging.getLogger(__name__)

def log(level):
    assert level in ("debug","info","warn","error")
    def decorator(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            write = getattr(logger, level)
            write('command: %s\n run time: %s seconds',args, time.time()-start)
            return result
        return wrapper
    return decorator
