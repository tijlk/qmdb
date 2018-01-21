import arrow


def humanized_time(timestamp, fallback="Not updated before"):
    if timestamp:
        return timestamp.humanize(arrow.now())
    else:
        return fallback
