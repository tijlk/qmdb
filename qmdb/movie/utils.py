import arrow


def humanized_time(timestamp):
    if timestamp:
        return timestamp.humanize(arrow.now())
    else:
        return "Not updated before"
