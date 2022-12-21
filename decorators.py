import datetime


def func_timer(func):
    def wrapper(*args, **kwargs):
        name = str(func.__name__)
        start = datetime.datetime.now()
        func(*args, **kwargs)
        end = datetime.datetime.now()
        print(name + ": " + str(end - start))

    return wrapper