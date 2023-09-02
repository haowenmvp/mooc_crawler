from datetime import datetime
from threading import Timer


def at(at_time: datetime, func, *args, **kwargs) -> Timer:
    now = datetime.now()
    if at_time <= now:
        interval = 0
    else:
        interval = (at_time - now).seconds
    t = Timer(interval, func, args=args, kwargs=kwargs)
    t.setName("TimerThread")
    t.start()
    return t
