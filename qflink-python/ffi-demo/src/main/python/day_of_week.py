from datetime import datetime


def day_of_week(date: str) -> int:
    """'1994-05-05' -> 4（星期一是 1，星期日是 7）"""
    return datetime.strptime(date, "%Y-%m-%d").isoweekday()
