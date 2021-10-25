import datetime


def get_now():
    return datetime.datetime.now()


def get_now_timestr():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
